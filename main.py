import asyncio
import aiohttp

import dash
import dash_html_components as html
import dash_core_components as dcc
from dash.dependencies import Input, Output
from sqlalchemy import create_engine
import pandas as pd
AlchemyEngine = create_engine('postgresql+psycopg2://tekieadmin@reports-tekie:E4qpTYr0oYt6uzq@reports-tekie.postgres.database.azure.com/tekie', pool_recycle=3600);
DBConnection = AlchemyEngine.connect()

app = dash.Dash()

headers = {
    'Authorization': 'ZXlKaGJHY2lPaUpJVXpJMU5pSXNJblI1Y0NJNklrcFhWQ0o5LmV5SmhjSEJKYm1adklqcDdJbTVoYldVaU9pSjBaV3RwWlZSdGN5SXNJblI1Y0dVaU9pSnpkR0YwYVdNaWZTd2lhV0YwSWpveE5UZzVOVE13T0RVNExDSmxlSEFpT2pFNU1EVXhNRFk0TlRoOS43ck9ILW5fM2Z1dUR0TWFoTVdJUVhOQS1nejllX216Y0o5VlNOTHd1WmVvOjpleUpoYkdjaU9pSklVekkxTmlJc0luUjVjQ0k2SWtwWFZDSjkuZXlKMWMyVnlTVzVtYnlJNmV5SnBaQ0k2SW1OcmVEZHdlbWc1YmpBd00zb3dkREZvWTNGaU1tUjZjbU1pTENKMWMyVnlibUZ0WlNJNklrOXRSSFZpWlhsNWVTSjlMQ0pwWVhRaU9qRTJOamN5TURnd09UZ3NJbVY0Y0NJNk1UWTVPRGMyTlRZNU9IMC4xcE1yeWNKUk5IWG9IbHRNVGU0eTZsb1RHaWNpNUM5T3RBcUtocHo1Vzdv'
}
async def get_meta_data():
    for i in range(4):
        df = pd.read_sql("""
            SELECT "id","studentId","studentName","userRole","studentGrade","studentSection","classroomId","classroomTitle","schoolName","courseTitle","sessionTitle","sessionType","sessionId","teacherName","courseCategory","sessionStart","sessionEnd","sessionStatus","studentAttendance"
            FROM "userSessionReport"
            WHERE "sessionStatus" IN ('completed','started')
            SKIP i * number_of_rows
            FIRST 2
        """, DBConnection)
        print(df)
    # do something with the data in the DataFrame df

    # sql_query = """
    #     SELECT COUNT(*)
    #     FROM "userLevelSessionReport"
    #     WHERE "sessionStatus" IN ('completed', 'started')
    #     """
    # df = pd.read_sql_query(sql_query, DBConnection)
    # return df['count']

async def make_query(session, query):
    async with session.post('https://api.tekie.com/graphql', json={'query': query}, headers=headers) as resp:
        return await resp.json()


# Create a layout with a div that will be used to trigger the callback
app.layout = html.Div([
    html.Div(id='trigger'),
    dcc.Store(id='local', storage_type='local'),

])

# Define a callback that starts the background tasks when the page loads
@app.callback(
    Output('local', 'data'),
    [Input('trigger', 'id')]
)
def start_background_tasks(trigger_id):
    async def main():
    #  Get the meta data
      meta_data = await get_meta_data()
      num_queries = meta_data // 5000
      print(num_queries)
      async with aiohttp.ClientSession() as session:
        tasks = []
        for i in range(4):
          query = """
          {
            userAssignments(first: 200, skip: %d) {
              id
              topic {
                title
              }
            }
          }
          """ % (i * 200)
          task = asyncio.create_task(make_query(session, query))
          print(task)
          tasks.append(task)
          print(tasks)

        results = await asyncio.gather(*tasks)
        print(results)
        
        return results
        # # results is a list of the results from all of the queries
        # # Store the results in local storage
        # for i, result in enumerate(results):
        #     js = 'localStorage.setItem("result_{}", "{}");'.format(i, result)
        #     app.callback(
        #         Output('', 'children'),
        #         [Input('', 'id')]
        #     )(js)

    return asyncio.run(main())
    raise dash.exceptions.PreventUpdate

if __name__ == '__main__':
    app.run_server()
