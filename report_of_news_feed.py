{# импортируем библиотеки
import telegram
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandahouse
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from datetime import datetime, timedelta

# напишем функция для запросов к ClickHouse
def Getch(query):
    connection = {
        'host': '*****',
        'password': '*****',
        'user': '*****',
        'database': '*****'}



    return pandahouse.read_clickhouse(query, connection=connection)



# напишем функцию которая будет отправлять отчет по ключевым метрикам в телеграм

# добавим id группы
group_chat_id='group_id'

def feed_action_report(chat=group_chat_id):
    chat_id = chat or 'my_chat_id'

    # сохраним токен и создадим бота
    my_token = 'bot_token'
    bot = telegram.Bot(token=my_token)

    # получим отчет по ключевым метрикам
    query_metrics = '''
    SELECT date,
           dau,
           views,
           likes,
           ctr,
           (dau/avg_dau - 1) AS dau_deviation_mean,
           (views/avg_views - 1) AS views_deviation_mean,
           (likes/avg_likes - 1) AS likes_deviation_mean,
           (ctr/avg_ctr - 1) AS ctr_deviation_mean
    FROM
    (SELECT toDate(time) AS date,
           COUNT(DISTINCT user_id) AS dau,
           countIf(user_id, action = 'view') AS views,
           countIf(user_id, action = 'like') AS likes,
           ROUND(likes / views, 3) AS ctr
    FROM simulator_20230220.feed_actions 
    WHERE date = today() - 1
    GROUP BY date) t1
    
    JOIN 
    (SELECT DISTINCT date,
           AVG(dau) OVER () AS avg_dau,
           AVG(views) OVER () AS avg_views,
           AVG(likes) OVER () AS avg_likes,
           AVG(ctr) OVER () AS avg_ctr
    FROM        
        (SELECT toDate(time) AS date,
                 COUNT(DISTINCT user_id) AS dau,
                 countIf(user_id, action = 'view') AS views,
                 countIf(user_id, action = 'like') AS likes,
                 ROUND(likes / views, 3) AS ctr
         FROM simulator_20230220.feed_actions 
         GROUP BY date) avg_table
    ) t2
           
    USING date  
    '''
    # сохраним данные в переменную
    data = Getch(query=query_metrics)

    # сформируем текстовое сообщение для телеграмма
    msg = f'''
    Отчет по Ленте новостей за {data.date.max().date()}:
    
    ____________________________
    Ключевые метрики:
    ____________________________
    DAU           {data.dau.max()}           
    Просмотры     {data.views.max()}         
    Лайки         {data.likes.max()}         
    CTR           {data.ctr.max():.2%}   
        
    ____________________________
    Отклонение от среднего значения:
    ____________________________
    DAU           {data.dau_deviation_mean.max():.2%}           
    Просмотры     {data.views_deviation_mean.max():.2%}         
    Лайки         {data.likes_deviation_mean.max():.2%}         
    CTR           {data.ctr_deviation_mean.max():.2%} 
    ____________________________
    '''

    # отправим сообщение
    bot.sendMessage(chat_id=chat_id, text=msg)

    # сформируем запрос для графика с ключевыми метриками за предыдущие 7 дней

    query_plots = '''
    SELECT toDate(time) AS date,
           COUNT(DISTINCT user_id) AS dau,
           countIf(user_id, action = 'view') AS views,
           countIf(user_id, action = 'like') AS likes,
           ROUND(likes / views, 3) AS ctr
    FROM simulator_20230220.feed_actions 
    WHERE date >= today() - 7 and date != today()
    GROUP BY date
    '''
    data = Getch(query=query_plots)

    # построим график

    # зададим стиль
    sns.set_style('whitegrid')

    # зададим фигуру и размер графика
    fig, ax = plt.subplots(2, 2, figsize=(18, 15))
    fig.suptitle(f'Ключевые метрики за период {data.date.min().date()} - {data.date.max().date()}', fontsize=25)

    # создадим список столбцов
    columns = data.drop(columns='date').columns
    # запишем переменные для упорядочивания графиков
    n, k = 0, 0
    # построим цикл графиков
    for column in columns:
        sns.lineplot(data=data, x='date', y=column, ax=ax[n, k])
        ax[n, k].set_title(column, size=19)
        k += 1
        if k == 2:
            k = 0
            n = 1
    # создадим файловый объект
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = 'metrics_plot.png'
    plt.close()

    bot.sendPhoto(chat_id=chat_id, photo=plot_object)



# Автоматизируем наш отчет

# зададим дефолтные параметры
default_args = {
    'owner': 'i-skljannyj',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 3, 25)
}

# Интервал запуска DAG
schedule_interval = '0 11 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def example_report():

    @task
    def test_report():
        feed_action_report()

    test_report()

example_report = example_report()