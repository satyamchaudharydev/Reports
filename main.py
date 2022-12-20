import dash
import dash_html_components as html
import dash_core_components as dcc
import aiopg

app = dash.Dash()

# Define the server-side callback function
@app.callback(
    dash.dependencies.Output('output-div', 'children'),
    [dash.dependencies.Input('url', 'pathname')])
async def update_output(pathname):
    # Connect to the database
    conn = await aiopg.connect(user='tekieadmin', password='E4qpTYr0oYt6uzq',
                                 host='reports-tekie.postgres.database.azure.com',
                                 port=5432, database='tekie')

    # Execute the SQL query and retrieve the results
    async with conn.cursor() as cur:
        
        await cur.execute("select \"id\",\"studentId\",\"studentName\",\"userRole\",\"studentGrade\",\"studentSection\",\"classroomId\",\"classroomTitle\",\"schoolName\",\"courseTitle\",\"sessionTitle\",\"sessionType\",\"sessionId\",\"teacherName\",\"courseCategory\",\"sessionStart\",\"sessionEnd\",\"sessionStatus\",\"studentAttendance\" from \"updatedUserSessionReport\"")
        results = await cur.fetchall()
        print(results)

    # Update the app's state with the result data
    return html.Div([html.Div(result) for result in results])

# Define the layout of the app
app.layout = html.Div(id='output-div')

if __name__ == '__main__':
    app.run_server(debug=True)

