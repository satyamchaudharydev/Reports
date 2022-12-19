import dash_html_components as html
import dash_core_components as dcc

def Login():
    html.Div([
    html.H1('Login Page'),
    html.Form([
        html.Label('Username'),
        dcc.Input(id='username', type='text', placeholder='Enter your username'),
        html.Label('Password'),
        dcc.Input(id='password', type='password', placeholder='Enter your password'),
        html.Button('Submit', id='submit-button')
    ])
])