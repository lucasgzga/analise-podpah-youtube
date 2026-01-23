''"""
ETL Projeto Podpah - YouTube Data Pipeline
Versão: 3.0 (Análise Temporal - Semestral/Anual)
"""

import os
import sys
import time
import logging
import pandas as pd
import sqlalchemy
import sqlite3
import isodate
from datetime import datetime, timedelta
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv
from tqdm import tqdm
from typing import List, Dict, Optional

# ========================================
# CONFIGURAÇÃO DE LOGGING
# ========================================
def setup_logging():
    """Configura logging profissional."""
    log_folder = 'logs'
    os.makedirs(log_folder, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = os.path.join(log_folder, f'etl_{timestamp}.log')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)


# ========================================
# CONFIGURAÇÕES
# ========================================
class Config:
    """Configuração centralizada."""
    
    def __init__(self):
        load_dotenv()
        self.api_key = os.getenv('YOUTUBE_API_KEY')
        self.channel_id = os.getenv('CHANNEL_ID', 'UCj9R9rOhl81fhnKxBpwJ-yw')
        self.db_path = 'data/banco_youtube.db'
        self.csv_output = 'data/dados_youtube_atual.csv'
        self.backup_folder = 'backups'
        self.max_retries = 3
        self.retry_delay = 5
        self.batch_size = 50
        self.quota_diaria = 10000  # Quota gratuita do YouTube
        
        self._validate()
    
    def _validate(self):
        if not self.api_key:
            raise ValueError("❌ ERRO: YOUTUBE_API_KEY não encontrada no .env")
        
        os.makedirs('data', exist_ok=True)
        os.makedirs(self.backup_folder, exist_ok=True)


# ========================================
# QUOTA TRACKER
# ========================================
class QuotaTracker:
    """Rastreador de consumo de quota da API."""
    
    CUSTOS = {
        'channels.list': 1,
        'playlistItems.list': 1,
        'videos.list': 1
    }
    
    def __init__(self, quota_diaria: int = 10000):
        self.quota_diaria = quota_diaria
        self.quota_usada = 0
        self.chamadas_detalhadas = []
    
    def registrar(self, tipo: str, quantidade: int = 1):
        """Registra chamada de API."""
        custo = self.CUSTOS.get(tipo, 1) * quantidade
        self.quota_usada += custo
        self.chamadas_detalhadas.append({
            'tipo': tipo,
            'quantidade': quantidade,
            'custo': custo,
            'timestamp': datetime.now()
        })
    
    def get_percentual(self) -> float:
        """Retorna percentual de quota usada."""
        return (self.quota_usada / self.quota_diaria) * 100
    
    def get_alerta(self) -> str:
        """Retorna alerta baseado no consumo."""
        perc = self.get_percentual()
        
        if perc >= 90:
            return f"🚨 CRÍTICO: {perc:.1f}% da quota diária!"
        elif perc >= 70:
            return f"⚠️  ATENÇÃO: {perc:.1f}% da quota usada"
        else:
            return f"✅ NORMAL: {perc:.1f}% da quota usada"
    
    def relatorio(self) -> str:
        """Gera relatório de quota."""
        return f"""
{'='*60}
📊 CONSUMO DE QUOTA API
{'='*60}
🔢 Quota usada: {self.quota_usada:,} / {self.quota_diaria:,} unidades
📈 Percentual: {self.get_percentual():.2f}%
📡 Total de chamadas: {len(self.chamadas_detalhadas)}
⚡ Quota restante: {self.quota_diaria - self.quota_usada:,} unidades

{self.get_alerta()}
{'='*60}
"""


# ========================================
# FUNÇÕES AUXILIARES
# ========================================
def retry_on_error(max_retries: int = 3, delay: int = 5):
    """Decorator para retry automático."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except HttpError as e:
                    if e.resp.status == 403:
                        logger.error(f"❌ Quota da API excedida: {e}")
                        raise
                    
                    wait_time = delay * (2 ** attempt)
                    logger.warning(f"⚠️  Tentativa {attempt + 1}/{max_retries} falhou. "
                                 f"Aguardando {wait_time}s...")
                    time.sleep(wait_time)
                    
                    if attempt == max_retries - 1:
                        logger.error(f"❌ Falha após {max_retries} tentativas")
                        raise
                except Exception as e:
                    logger.error(f"❌ Erro inesperado: {e}")
                    raise
        return wrapper
    return decorator


def validate_dataframe(df: pd.DataFrame) -> bool:
    """Valida schema e qualidade dos dados."""
    required_columns = ['Video_ID', 'Titulo', 'Data_Publicacao', 'Views', 
                       'Likes', 'Comentarios', 'Thumbnail_URL']
    
    missing_cols = set(required_columns) - set(df.columns)
    if missing_cols:
        logger.error(f"❌ Colunas faltando: {missing_cols}")
        return False
    
    if df['Video_ID'].duplicated().any():
        logger.warning("⚠️  IDs duplicados. Removendo...")
        df.drop_duplicates(subset='Video_ID', inplace=True)
    
    if df['Views'].isnull().any():
        logger.warning("⚠️  Views nulas. Preenchendo com 0...")
        df['Views'].fillna(0, inplace=True)
    
    logger.info(f"✅ Validação OK: {len(df)} registros válidos")
    return True


# ========================================
# CLASSE PRINCIPAL ETL
# ========================================
class YouTubeETL:
    """Pipeline ETL com histórico temporal."""
    
    def __init__(self, config: Config):
        self.config = config
        self.youtube = build('youtube', 'v3', developerKey=config.api_key)
        self.quota_tracker = QuotaTracker(config.quota_diaria)
        self.stats = {
            'videos_coletados': 0,
            'tempo_inicio': datetime.now(),
            'data_execucao': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
    
    @retry_on_error(max_retries=3, delay=5)
    def get_channel_info(self) -> str:
        """Busca informações do canal."""
        logger.info("🔍 Buscando informações do canal...")
        
        request = self.youtube.channels().list(
            part='contentDetails,statistics,snippet',
            id=self.config.channel_id
        )
        response = request.execute()
        self.quota_tracker.registrar('channels.list')
        
        if not response.get('items'):
            raise ValueError(f"Canal não encontrado: {self.config.channel_id}")
        
        channel_data = response['items'][0]
        uploads_id = channel_data['contentDetails']['relatedPlaylists']['uploads']
        subs_count = int(channel_data['statistics']['subscriberCount'])
        channel_name = channel_data['snippet']['title']
        
        logger.info(f"📺 Canal: {channel_name}")
        logger.info(f"👥 Inscritos: {subs_count:,}")
        
        return uploads_id
    
    @retry_on_error(max_retries=3, delay=5)
    def get_all_video_ids(self, uploads_id: str) -> List[str]:
        """Coleta todos os IDs de vídeos."""
        video_ids = []
        next_page_token = None
        
        logger.info("🔍 Coletando lista de vídeos...")
        
        with tqdm(desc="Páginas processadas", unit="pág") as pbar:
            while True:
                request = self.youtube.playlistItems().list(
                    part='contentDetails',
                    playlistId=uploads_id,
                    maxResults=self.config.batch_size,
                    pageToken=next_page_token
                )
                response = request.execute()
                self.quota_tracker.registrar('playlistItems.list')
                
                for item in response['items']:
                    video_ids.append(item['contentDetails']['videoId'])
                
                pbar.update(1)
                pbar.set_postfix({'Vídeos': len(video_ids)})
                
                next_page_token = response.get('nextPageToken')
                if not next_page_token:
                    break
        
        logger.info(f"✅ Total de vídeos: {len(video_ids)}")
        self.stats['videos_coletados'] = len(video_ids)
        return video_ids
    
    @retry_on_error(max_retries=3, delay=5)
    def get_video_details(self, video_ids: List[str]) -> pd.DataFrame:
        """Busca detalhes completos dos vídeos."""
        video_data = []
        total_batches = (len(video_ids) + self.config.batch_size - 1) // self.config.batch_size
        
        logger.info(f"📊 Coletando detalhes ({total_batches} lotes)...")
        
        with tqdm(total=len(video_ids), desc="Vídeos processados", unit="vídeo") as pbar:
            for i in range(0, len(video_ids), self.config.batch_size):
                batch_ids = video_ids[i:i + self.config.batch_size]
                ids_string = ','.join(batch_ids)
                
                request = self.youtube.videos().list(
                    part='snippet,statistics,contentDetails',
                    id=ids_string
                )
                response = request.execute()
                self.quota_tracker.registrar('videos.list')
                
                for item in response['items']:
                    try:
                        video_data.append(self._parse_video_item(item))
                    except Exception as e:
                        logger.warning(f"⚠️  Erro ao processar {item['id']}: {e}")
                        continue
                
                pbar.update(len(batch_ids))
        
        return pd.DataFrame(video_data)
    
    def _parse_video_item(self, item: Dict) -> Dict:
        """Extrai dados de um vídeo."""
        stats = item.get('statistics', {})
        snippet = item.get('snippet', {})
        content_details = item.get('contentDetails', {})
        
        # Duração
        duracao_iso = content_details.get('duration', 'PT0S')
        try:
            duracao_segundos = int(isodate.parse_duration(duracao_iso).total_seconds())
        except:
            duracao_segundos = 0
        
        # Thumbnail
        thumbnails = snippet.get('thumbnails', {})
        thumbnail_url = (
            thumbnails.get('maxres', {}).get('url') or
            thumbnails.get('high', {}).get('url') or
            thumbnails.get('medium', {}).get('url') or
            thumbnails.get('default', {}).get('url') or
            f"https://img.youtube.com/vi/{item['id']}/hqdefault.jpg"
        )
        
        return {
            'Video_ID': item['id'],
            'Titulo': snippet.get('title', 'Sem título'),
            'Data_Publicacao': snippet.get('publishedAt'),
            'Views': int(stats.get('viewCount', 0)),
            'Likes': int(stats.get('likeCount', 0)),
            'Comentarios': int(stats.get('commentCount', 0)),
            'Duracao_ISO': duracao_iso,
            'Duracao_Segundos': duracao_segundos,
            'Duracao_Formatada': str(timedelta(seconds=duracao_segundos)),
            'Thumbnail_URL': thumbnail_url,
            'Data_Coleta': self.stats['data_execucao']
        }
    
    def save_data(self, df: pd.DataFrame):
        """Salva dados com histórico temporal."""
        if not validate_dataframe(df):
            raise ValueError("Dados inválidos. Operação cancelada.")
        
        df['Data_Publicacao'] = pd.to_datetime(df['Data_Publicacao'])
        df['Data_Simples'] = df['Data_Publicacao'].dt.date
        
        # CSV snapshot atual
        df.to_csv(self.config.csv_output, index=False)
        logger.info(f"✅ CSV salvo: {self.config.csv_output}")
        
        # Backup CSV com timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_csv = os.path.join(self.config.backup_folder, f"snapshot_{timestamp}.csv")
        df.to_csv(backup_csv, index=False)
        logger.info(f"💾 Backup salvo: {backup_csv}")
        
        # SQLite com estrutura temporal
        engine = sqlalchemy.create_engine(f'sqlite:///{self.config.db_path}')
        
        # Tabela snapshot atual (sempre substitui)
        df.to_sql('videos_stats_atual', engine, if_exists='replace', index=False)
        
        # Tabela histórico (acumula todas execuções)
        df.to_sql('videos_historico', engine, if_exists='append', index=False)
        
        # Log de execução
        self._save_execution_log(engine, df)
        
        logger.info(f"✅ Banco atualizado: {self.config.db_path}")
        logger.info(f"   📊 Snapshot atual: {len(df)} registros")
        logger.info(f"   📈 Histórico: Dados acumulados")
    
    def _save_execution_log(self, engine, df: pd.DataFrame):
        """Salva log de execução no banco."""
        tempo_total = (datetime.now() - self.stats['tempo_inicio']).total_seconds()
        
        log_data = pd.DataFrame([{
            'Data_Execucao': self.stats['data_execucao'],
            'Videos_Coletados': len(df),
            'Chamadas_API': len(self.quota_tracker.chamadas_detalhadas),
            'Quota_Usada': self.quota_tracker.quota_usada,
            'Quota_Percentual': self.quota_tracker.get_percentual(),
            'Tempo_Execucao_Segundos': tempo_total,
            'Total_Views': df['Views'].sum(),
            'Total_Likes': df['Likes'].sum(),
            'Total_Comentarios': df['Comentarios'].sum()
        }])
        
        log_data.to_sql('execucoes_log', engine, if_exists='append', index=False)
    
    def generate_report(self):
        """Gera relatório completo de execução."""
        tempo_total = (datetime.now() - self.stats['tempo_inicio']).total_seconds()
        
        report = f"""
{'='*60}
🎯 RELATÓRIO DE EXECUÇÃO - ETL PODPAH
{'='*60}
📅 Data: {self.stats['data_execucao']}
⏱️  Tempo total: {tempo_total:.2f}s
📹 Vídeos coletados: {self.stats['videos_coletados']:,}
⚡ Média: {tempo_total/max(self.stats['videos_coletados'], 1):.2f}s/vídeo
{'='*60}
"""
        logger.info(report)
        logger.info(self.quota_tracker.relatorio())


# ========================================
# INICIALIZAÇÃO DO BANCO
# ========================================
def init_database(db_path: str):
    """Cria estrutura do banco na primeira execução."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Verifica se tabelas existem antes de criar índices
    cursor.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name='videos_historico'
    """)
    
    if cursor.fetchone():
        # Só cria índices se a tabela já existir
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_video_id 
            ON videos_historico(Video_ID)
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_data_coleta 
            ON videos_historico(Data_Coleta)
        """)
        
        logger.info("✅ Índices do banco criados/verificados")
    
    conn.commit()
    conn.close()


# ========================================
# EXECUÇÃO PRINCIPAL
# ========================================
def main():
    """Execução principal do ETL."""
    global logger
    logger = setup_logging()
    
    try:
        logger.info("="*60)
        logger.info("🚀 ETL PODPAH - ANÁLISE TEMPORAL (SEMESTRAL/ANUAL)")
        logger.info("="*60)
        
        config = Config()
        etl = YouTubeETL(config)
        
        # Inicializa banco
        init_database(config.db_path)
        
        # Pipeline ETL
        uploads_id = etl.get_channel_info()
        video_ids = etl.get_all_video_ids(uploads_id)
        df_videos = etl.get_video_details(video_ids)
        etl.save_data(df_videos)
        
        etl.generate_report()
        
        logger.info("="*60)
        logger.info("✅ ETL CONCLUÍDO COM SUCESSO!")
        logger.info("="*60)
        logger.info("\n💡 PRÓXIMA EXECUÇÃO:")
        logger.info("   📅 Sugerido: 6 meses (análise semestral)")
        logger.info("   📊 Dados históricos preservados para comparação")
        
    except Exception as e:
        logger.error(f"❌ ERRO CRÍTICO: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()