-- ========================================
-- QUERIES PARA ANÁLISE ATUAL (SNAPSHOT HOJE)
-- Use no Power BI ou DB Browser SQLite
-- ========================================

-- 1. TOP 50 VÍDEOS MAIS VISTOS (COM THUMBNAIL)
SELECT 
    Video_ID,
    Titulo,
    Data_Publicacao,
    Views,
    Likes,
    Comentarios,
    Duracao_Formatada,
    Thumbnail_URL,
    ROUND((Likes * 100.0 / NULLIF(Views, 0)), 2) as Taxa_Curtidas,
    ROUND((Comentarios * 100.0 / NULLIF(Views, 0)), 2) as Taxa_Comentarios,
    ROUND(((Likes + Comentarios) * 100.0 / NULLIF(Views, 0)), 2) as Taxa_Engajamento
FROM videos_stats_atual
ORDER BY Views DESC
LIMIT 50;


-- 2. TOP 50 VÍDEOS MAIS CURTIDOS
SELECT 
    Video_ID,
    Titulo,
    Data_Publicacao,
    Likes,
    Views,
    Comentarios,
    Thumbnail_URL,
    ROUND((Likes * 100.0 / NULLIF(Views, 0)), 2) as Percentual_Likes
FROM videos_stats_atual
ORDER BY Likes DESC
LIMIT 50;


-- 3. TOP 50 VÍDEOS MAIS COMENTADOS
SELECT 
    Video_ID,
    Titulo,
    Data_Publicacao,
    Comentarios,
    Views,
    Likes,
    Thumbnail_URL,
    ROUND((Comentarios * 100.0 / NULLIF(Views, 0)), 2) as Percentual_Comentarios
FROM videos_stats_atual
ORDER BY Comentarios DESC
LIMIT 50;


-- 4. CORRELAÇÃO: VIEWS x LIKES x COMENTÁRIOS
-- Vídeos que estão em TODOS os Top 100
WITH top_views AS (
    SELECT Video_ID FROM videos_stats_atual ORDER BY Views DESC LIMIT 100
),
top_likes AS (
    SELECT Video_ID FROM videos_stats_atual ORDER BY Likes DESC LIMIT 100
),
top_comentarios AS (
    SELECT Video_ID FROM videos_stats_atual ORDER BY Comentarios DESC LIMIT 100
)
SELECT 
    v.Video_ID,
    v.Titulo,
    v.Views,
    v.Likes,
    v.Comentarios,
    v.Thumbnail_URL,
    CASE 
        WHEN v.Video_ID IN (SELECT Video_ID FROM top_views) THEN '✅' 
        ELSE '❌' 
    END as Top_Views,
    CASE 
        WHEN v.Video_ID IN (SELECT Video_ID FROM top_likes) THEN '✅' 
        ELSE '❌' 
    END as Top_Likes,
    CASE 
        WHEN v.Video_ID IN (SELECT Video_ID FROM top_comentarios) THEN '✅' 
        ELSE '❌' 
    END as Top_Comentarios
FROM videos_stats_atual v
WHERE 
    v.Video_ID IN (SELECT Video_ID FROM top_views)
    OR v.Video_ID IN (SELECT Video_ID FROM top_likes)
    OR v.Video_ID IN (SELECT Video_ID FROM top_comentarios)
ORDER BY v.Views DESC;


-- 5. MAIOR ENGAJAMENTO (LIKES + COMENTÁRIOS / VIEWS)
SELECT 
    Video_ID,
    Titulo,
    Data_Publicacao,
    Views,
    Likes,
    Comentarios,
    Thumbnail_URL,
    ROUND(((Likes + Comentarios) * 100.0 / NULLIF(Views, 0)), 2) as Taxa_Engajamento_Total,
    ROUND((Likes * 100.0 / NULLIF(Views, 0)), 2) as Taxa_Likes,
    ROUND((Comentarios * 100.0 / NULLIF(Views, 0)), 2) as Taxa_Comentarios
FROM videos_stats_atual
WHERE Views > 10000  -- Filtrar vídeos com audiência mínima
ORDER BY Taxa_Engajamento_Total DESC
LIMIT 50;


-- 6. EVOLUÇÃO HISTÓRICA POR ANO
SELECT 
    strftime('%Y', Data_Publicacao) as Ano,
    COUNT(*) as Total_Videos,
    SUM(Views) as Total_Views,
    SUM(Likes) as Total_Likes,
    SUM(Comentarios) as Total_Comentarios,
    ROUND(AVG(Views), 0) as Media_Views,
    ROUND(AVG(Likes), 0) as Media_Likes,
    ROUND(AVG(Comentarios), 0) as Media_Comentarios,
    ROUND(AVG((Likes + Comentarios) * 100.0 / NULLIF(Views, 0)), 2) as Taxa_Engajamento_Media
FROM videos_stats_atual
GROUP BY strftime('%Y', Data_Publicacao)
ORDER BY Ano;


-- 7. EVOLUÇÃO POR MÊS (ÚLTIMOS 24 MESES)
SELECT 
    strftime('%Y-%m', Data_Publicacao) as Mes_Ano,
    COUNT(*) as Videos_Publicados,
    SUM(Views) as Total_Views,
    SUM(Likes) as Total_Likes,
    SUM(Comentarios) as Total_Comentarios,
    ROUND(AVG(Views), 0) as Media_Views,
    ROUND(AVG(Duracao_Segundos/60.0), 1) as Duracao_Media_Minutos
FROM videos_stats_atual
WHERE Data_Publicacao >= date('now', '-24 months')
GROUP BY strftime('%Y-%m', Data_Publicacao)
ORDER BY Mes_Ano;


-- 8. VÍDEOS MAIS LONGOS vs MAIS CURTOS (PERFORMANCE)
SELECT 
    'Vídeos Longos (>60min)' as Categoria,
    COUNT(*) as Quantidade,
    ROUND(AVG(Views), 0) as Media_Views,
    ROUND(AVG(Likes), 0) as Media_Likes,
    ROUND(AVG(Comentarios), 0) as Media_Comentarios,
    ROUND(AVG((Likes + Comentarios) * 100.0 / NULLIF(Views, 0)), 2) as Taxa_Engajamento
FROM videos_stats_atual
WHERE Duracao_Segundos > 3600

UNION ALL

SELECT 
    'Vídeos Médios (30-60min)' as Categoria,
    COUNT(*) as Quantidade,
    ROUND(AVG(Views), 0) as Media_Views,
    ROUND(AVG(Likes), 0) as Media_Likes,
    ROUND(AVG(Comentarios), 0) as Media_Comentarios,
    ROUND(AVG((Likes + Comentarios) * 100.0 / NULLIF(Views, 0)), 2) as Taxa_Engajamento
FROM videos_stats_atual
WHERE Duracao_Segundos BETWEEN 1800 AND 3600

UNION ALL

SELECT 
    'Vídeos Curtos (<30min)' as Categoria,
    COUNT(*) as Quantidade,
    ROUND(AVG(Views), 0) as Media_Views,
    ROUND(AVG(Likes), 0) as Media_Likes,
    ROUND(AVG(Comentarios), 0) as Media_Comentarios,
    ROUND(AVG((Likes + Comentarios) * 100.0 / NULLIF(Views, 0)), 2) as Taxa_Engajamento
FROM videos_stats_atual
WHERE Duracao_Segundos < 1800;


-- 9. PRIMEIROS 10 VÍDEOS DO CANAL vs ÚLTIMOS 10
WITH primeiros AS (
    SELECT * FROM videos_stats_atual
    ORDER BY Data_Publicacao ASC
    LIMIT 10
),
ultimos AS (
    SELECT * FROM videos_stats_atual
    ORDER BY Data_Publicacao DESC
    LIMIT 10
)
SELECT 
    'Primeiros 10 Vídeos' as Grupo,
    ROUND(AVG(Views), 0) as Media_Views,
    ROUND(AVG(Likes), 0) as Media_Likes,
    ROUND(AVG(Comentarios), 0) as Media_Comentarios,
    MIN(Data_Publicacao) as Data_Inicio,
    MAX(Data_Publicacao) as Data_Fim
FROM primeiros

UNION ALL

SELECT 
    'Últimos 10 Vídeos' as Grupo,
    ROUND(AVG(Views), 0) as Media_Views,
    ROUND(AVG(Likes), 0) as Media_Likes,
    ROUND(AVG(Comentarios), 0) as Media_Comentarios,
    MIN(Data_Publicacao) as Data_Inicio,
    MAX(Data_Publicacao) as Data_Fim
FROM ultimos;


-- 10. VÍDEOS "VIRAIS" (OUTLIERS DE PERFORMANCE)
-- Vídeos que performaram muito acima da média
WITH stats AS (
    SELECT 
        AVG(Views) as media_views,
        AVG(Likes) as media_likes,
        AVG(Comentarios) as media_comentarios
    FROM videos_stats_atual
)
SELECT 
    v.Video_ID,
    v.Titulo,
    v.Data_Publicacao,
    v.Views,
    v.Likes,
    v.Comentarios,
    v.Thumbnail_URL,
    ROUND(v.Views / s.media_views, 2) as Multiplo_Media_Views,
    ROUND(v.Likes / s.media_likes, 2) as Multiplo_Media_Likes
FROM videos_stats_atual v, stats s
WHERE 
    v.Views > (s.media_views * 2)  -- Mais de 2x a média
    OR v.Likes > (s.media_likes * 2)
ORDER BY v.Views DESC
LIMIT 50;


-- 11. DASHBOARD EXECUTIVO - KPIs PRINCIPAIS
SELECT 
    COUNT(*) as Total_Videos,
    SUM(Views) as Total_Views_Canal,
    SUM(Likes) as Total_Likes_Canal,
    SUM(Comentarios) as Total_Comentarios_Canal,
    ROUND(AVG(Views), 0) as Media_Views_Por_Video,
    ROUND(AVG(Likes), 0) as Media_Likes_Por_Video,
    ROUND(AVG(Comentarios), 0) as Media_Comentarios_Por_Video,
    ROUND(AVG((Likes + Comentarios) * 100.0 / NULLIF(Views, 0)), 2) as Taxa_Engajamento_Media,
    ROUND(AVG(Duracao_Segundos/60.0), 1) as Duracao_Media_Minutos,
    MIN(Data_Publicacao) as Primeiro_Video,
    MAX(Data_Publicacao) as Ultimo_Video,
    ROUND(JULIANDAY('now') - JULIANDAY(MIN(Data_Publicacao)), 0) as Dias_Desde_Primeiro_Video
FROM videos_stats_atual;


-- 12. ANÁLISE DE CORRELAÇÃO SIMPLIFICADA
-- Análise visual de relação entre métricas (use gráfico de dispersão no Power BI)
SELECT 
    Video_ID,
    Titulo,
    Views,
    Likes,
    Comentarios,
    ROUND((Likes * 100.0 / NULLIF(Views, 0)), 2) as Percentual_Likes,
    ROUND((Comentarios * 100.0 / NULLIF(Views, 0)), 2) as Percentual_Comentarios,
    Thumbnail_URL
FROM videos_stats_atual
WHERE Views > 1000  -- Filtrar ruído
ORDER BY Views DESC;

-- Nota: Para correlação estatística, use DAX no Power BI:
-- Correlacao = CORREL(Tabela[Views], Tabela[Likes])


-- 13. CRESCIMENTO MENSAL DE PUBLICAÇÕES (Versão Compatível)
WITH mensal AS (
    SELECT 
        strftime('%Y-%m', Data_Publicacao) as Mes,
        COUNT(*) as Videos_Publicados
    FROM videos_stats_atual
    GROUP BY strftime('%Y-%m', Data_Publicacao)
)
SELECT 
    m1.Mes,
    m1.Videos_Publicados,
    m2.Videos_Publicados as Videos_Mes_Anterior,
    (m1.Videos_Publicados - COALESCE(m2.Videos_Publicados, 0)) as Variacao
FROM mensal m1
LEFT JOIN mensal m2 
    ON m2.Mes = date(m1.Mes || '-01', '-1 month', 'start of month', '%Y-%m')
ORDER BY m1.Mes DESC
LIMIT 24;