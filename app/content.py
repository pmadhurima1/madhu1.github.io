from flask import redirect
import dash
import dash_table
import pandas as pd
import dash_html_components as html
import dash_core_components as dcc
from dash.dependencies import Input, Output,State
import psycopg2
import socket
import plotly.express as px
import plotly.graph_objects as go


def shorttable():
    #either:
    #selected_rows=[rows[i] for i in selected_row_indices]
    #or
    #selected_rows=pd.DataFrame(rows).iloc[i]
    #selected_rows=[rows[i] for i in selected_row_indices]
    return html.H1(children='Selected rows ', style={'textAlign': 'left',
            })



def get_userinfo(search_login):
    if search_login=='':
        return  html.Div(children='Enter login_id  you like to search', style={'textAlign': 'left',
            })
    
    df=get_userdata(search_login);
    df=df.drop(columns=['id'])

    #colors = ['#7FDBFF' if i in df['commit'] else '#0074D9'
    #          for i in range(len(df))]
    color_theme=[
                                    'rgb(0, 204, 0)',
                                    'rgb(255,0,255)',
                                    'rgb(118, 17, 195)',
                                    'rgb(0, 48, 240)',
                                    'rgb(240, 88, 0)',
                                    'rgb(215, 11, 11)',
                                    'rgb(11, 133, 215)',
                                    'rgb(255, 255, 0)',
                                    'rgb(0, 255, 0)',
                                    'rgb(0, 0, 255)'
                ]


    #labels = ['Oxygen','Hydrogen','Carbon_Dioxide','Nitrogen']
    #values = [4500, 2500, 1053, 500]
    value=1
    #dt=[]
    dtlist=[]
    #for val in df['score']
    #    dt.add(val)
    #dtlist.add(df['score'])
    piedata = [
        {
            'values': [df['score']/sum(df['score'])*100][int(value)-1],
            'type': 'pie',
            'labels': df['language'],
            'marker': {
                        'colors': color_theme
                        },
        }
    ]
        



    return  html.Div([
            html.Div([    
            dash_table.DataTable(
                id='datatable-userinfo',
                columns=[ {"name": i, "id": i, "deletable": True, "selectable": True} for i in df.columns],
                data=df.to_dict('records'),
                editable=True,
                filter_action="native",
                sort_action="native",
                sort_mode="multi",
                column_selectable="single",
                row_selectable="single",
                row_deletable=True,
                selected_columns=['score'],
                selected_rows=[0],
                style_cell_conditional=[
                    {'if': {'column_id': c},
                            'display': 'none'
                    } for c in ['proj_owned','commit','issue']
                ],
                #hidden_columns=['id','login'],
                style_cell={'fontSize':20, 'font-family':'sans-serif'},
                page_action="native",
                page_current= 0,
                page_size= 10,
                )
            
            ])
            ,
            
            
                
            #html.Div( id="bar" [    
                
            #px.bar(tips, x="score", y="language", color='day', orientation='h',
            #hover_data=["tip", "size"],
            #height=400,
            #title='GH score')
            #fig.show()
            #dff = df if rows is None else pd.DataFrame(rows)
            #column=dff["commit"]    
            #]
            #)
            html.Div([
            html.Div([

            html.Div([
            dcc.Graph(
                id="hbar-commit",
            
                figure={
                      "data": [
                      {
                            "y":  df["commit"],
                            "x": df["language"],
                            "type": "bar",
                            "marker": {"color": color_theme},
                    }
                    ],
                    "layout": {
                    "xaxis": {"automargin": True},
                    "yaxis": {
                        "automargin": True,
                        "title": {"text": "Commit"}
                    },
                    "height": 200,
                    "margin": {"t": 10, "l": 10, "r": 100},
                   },
                }
            ),


            dcc.Graph(
                id="hbar-issue",

                figure={
                      "data": [
                      {
                            "y":  df["issue"],
                            "x": df["language"],
                            "type": "bar",
                            "marker": {"color": color_theme
                                },
                    }
                    ],
                    "layout": {
                    "xaxis": {"automargin": True},
                    "yaxis": {
                        "automargin": True,
                        "title": {"text": "issue"}
                    },
                    "height": 200,
                    "margin": {"t": 10, "l": 10, "r": 100},
                   },
                }
            ),
            dcc.Graph(
                id="hbar-projown",

                figure={
                      "data": [
                      {
                            "y":  df["proj_owned"],
                            "x": df["language"],
                            "type": "bar",
                            "marker": {"color":color_theme},
                       }
                    ],
                    "layout": {
                    "xaxis": {"automargin": True},
                    "yaxis": {
                        "automargin": True,
                        "title": {"text": "projects owned"}
                    },
                    "height": 200,
                    "margin": {"t": 10, "l": 10, "r": 100},
                   },
                }
            )

            


            ]
            , style={'width': '49%', 'display': 'inline-block'}),
    html.Div([

        dcc.Graph(
                id="hbar-score",

                figure={
                      "data": [
                      {
                            "y":  df["score"],
                            "x": df["language"],
                            "type": "bar",
                            "marker": {"color":color_theme},
                    }
                    ],
                    "layout": {
                    "xaxis": {"automargin": True},
                    "yaxis": {
                        "automargin": True,
                        "title": {"text": "Score"}
                    },
                    "height": 200,
                    "margin": {"t": 10, "l": 10, "r": 100},
                   },
                }
            ),

        
            dcc.Graph(
            id='pie',
            figure={
                'data': piedata,
                'layout': {
                    #'title': "Expertise",
                    'margin': {
                        'l': 30,
                        'r': 10,
                        'b': 50,
                        't': 10
                    },

                    'legend': {'x': 0, 'y': 1 , "orientation": "h", "xanchor": "center","yanchor": "bottom"},
                    
                    #'legend': {df['language']}
                    }
                }       
            )

            ], style={'width': '49%', 'display': 'inline-block','vertical-align': 'top'})
            ],style={"border":"2px black solid"})
            ])

            ],style={"width":"99%",'display': 'inline-block'}
            )
           # # check if column exists - user may have deleted it
           # # If `column.deletable=False`, then you don't
           # # need to do this check.
           # #for column in ["commit", "issue", "score"] if column in df
            
        
            


    

def get_tabcontent():
        return html.Div (children=[
            html.H3(children='Search potential Candidates.', style={
            'textAlign': 'center',
            }),
            html.Div(
                [
                dcc.Dropdown(id='city_input', style={'height': '30px', 'width': '200px'},
                options=[
                    {'label': 'New York City', 'value': 'new york'},
                    {'label': 'San Francisco', 'value': 'san francisco'},
                    {'label': 'Seattle', 'value': 'seattle'}
                ],
                value='seattle'
                ),
                html.Br(),
                dcc.Dropdown(id='language_input', style={'height': '30px', 'width': '200px'},
                options=[
                {'label': 'Python', 'value': 'python'},
                {'label': 'C++', 'value': 'c++'},
                {'label': 'Java', 'value': 'java'},
                {'label': 'Go', 'value': 'ruby'},
                {'label': 'Jupyter', 'value': 'jupyter notebook'},
                {'label': 'HTML', 'value': 'html'},
                {'label': 'CSS', 'value': 'css'},
                {'label': 'C#', 'value': 'c#'},


                ],
                value='python'
                )
                , 
                html.Br(),
                #html.Div(id="table-output")
                html.Button(id='submit_button', n_clicks=0, children='Submit'),
                html.Div(id='output_state'),
                html.Br(),
                ]
            ),
            html.Div([

            dash_table.DataTable(
                id='datatable-interactivity'#,
                #columns=[ {"name": i, "id": i, "deletable": True, "selectable": True} for i in df.columns],
                #data=df.to_dict('records'),
                #editable=True,
                #filter_action="native",
                #sort_action="native",
                #sort_mode="multi",
                #column_selectable="single",
                #row_selectable="multi",
                #row_deletable=True,
                #selected_columns=[],
                #selected_rows=[],
                #page_action="native",
                #page_current= 0,
                #page_size= 10,
                )
            ])
],style={"width":"90%",'display': 'inline-block', 'padding-left':'5%', 'padding-right':'5%'})

def get_usercontent():
    return html.Div([
        dcc.Input(id='username-id', value='', type="text"),
        html.Button('search', id='button'),
        html.Div(id='username-div')#,
         #dcc.Graph(
         #       id='cx1'
         #       )
        ],style={"width":"90%",'display': 'inline-block', 'padding-left':'5%', 'padding-right':'5%'})


def get_userdata(search_login):
    connection = psycopg2.connect(host = '100.21.173.99', port = '5432', database = 'resultdb', user = 'dbuser', password = 'passwd')
    query = "SELECT users.id, users.login, ghstat.proj_owned, ghstat.language, ghstat.commit, ghstat.issue FROM ghstat  JOIN users ON users.id=ghstat.uid  WHERE users.login LIKE '%"+search_login+"%' ;"
    data1 = pd.read_sql_query(query, connection)
    data1=data1.replace(to_replace =["ruby"],  
                            value ="go") 

        
    data1['score'] = data1.proj_owned*20+ data1.commit + 3*data1.issue
    #data1['score']= data1.score/data1['score'].max()*100
    data1.score = data1.score.round()
    data1=data1.sort_values(by=['score'],ascending=False)
    return data1;


def get_candidate_info(language_input, city_input):
    connection = psycopg2.connect(host ='100.21.173.99', port = '5432', database = 'resultdb', user = 'dbuser', password = 'passwd')
    query = "select users.id, users.login,ghstat.proj_owned, ghstat.commit, ghstat.issue from ghstat  join users on users.id=ghstat.uid  where ghstat.language = '" + language_input + "' AND users.city='"+city_input+"';"
    data1 = pd.read_sql_query(query, connection)
    data1['score'] = data1.proj_owned*20+ data1.commit + 3*data1.issue
    #data1['score']= data1.score/data1['score'].max()*100
    data1.score = data1.score.round()
    data1=data1.sort_values(by=['score'],ascending=False)
    return data1;    


