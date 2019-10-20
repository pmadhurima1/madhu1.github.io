#!/usr/bin/python
# -*- coding: utf-8 -*-
import dash
import dash_table
import pandas as pd
import dash_html_components as html
import dash_core_components as dcc
from dash.dependencies import Input, Output,State
import psycopg2
import socket
from content import get_tabcontent,get_candidate_info,get_usercontent, get_userinfo, shorttable

#setting up dash
#external_stylesheets = ['https://codepen.io/anon/pen/mardKv.css']
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']


external_css = ["https://codepen.io/chriddyp/pen/bWLwgP.css",
                "main.css"
                #"https://cdnjs.cloudflare.com/ajax/libs/normalize/7.0.0/normalize.min.css",
                #"https://cdnjs.cloudflare.com/ajax/libs/skeleton/2.0.4/skeleton.min.css",
                #"//fonts.googleapis.com/css?family=Raleway:400,300,600",
                # "https://cdn.rawgit.com/plotly/dash-app-stylesheets/5047eb29e4afe01b45b27b1d2f7deda2a942311a/goldman-sachs-report.css",
                #'https://codepen.io/plotly/pen/YEYMBZ.css'
                #"https://maxcdn.bootstrapcdn.com/font-awesome/4.7.0/css/font-awesome.min.css"
                ]

#for css in external_css:
#    app.css.append_css({"external_url": css})
app = dash.Dash(__name__, external_stylesheets=external_css)
app.config['suppress_callback_exceptions']=True
#for css in external_css:
#    app.css.append_css({"external_url": css})
server=app.server




colors = {
        'background': '#C0C0C0',
        'text': '#00004D'
    }

app.layout =html.Div([
    html.H1('<ACE_coder/>:',style={'text-align': 'center', 'background':colors['text'],'color':colors['background']}
   # html.P('Recruitment tool to find candidates from  GitHub users')
    )
    ,
    html.H4('Recruitment tool to find candidates from  GitHub users',style={'text-align': 'center'} )
    ,
    dcc.Tabs(id="tabs-navigation", value='topic-search', children=[
        dcc.Tab(label='Potential Candidates', value='candidate-search', id='candidate-search'),
        dcc.Tab(label='User Search', value='user-search', id='user-search'),
    ]
    
    ),
    html.Div(id='search-tabs')
],style={"width":"90%",'display': 'inline-block', 'padding-left':'5%', 'padding-right':'5%','color': colors['text']}
)


DF_ghstat=[];



@app.callback(Output('search-tabs', 'children'),
              [Input('tabs-navigation', 'value')]
    )

def render_content(tab):
    if tab == 'candidate-search':
        return get_tabcontent()
    if tab== 'user-search':
        #dff = filter_data(selected_row_indices)
        return get_usercontent()  


@app.callback(
    Output(component_id='username-div', component_property='children'),
    [Input('button','n_clicks')],
    state=[State(component_id='username-id', component_property='value')]
)
def update_output_div(n_clicks, input_value):
    return get_userinfo(input_value) 
    #return 'You\'ve entered "{}" and clicked {} times'.format(input_value, n_clicks)


def filter_data(selected_row_indices):
        dff = DF_ghstat.iloc[selected_row_indices]
        return dff



@app.callback(
    Output('usertable-div','children'),
    [
     dash.dependencies.Input('datatable-interactivity', 'selected_row_indices')])
def rows_update(rows,selected_row_indices):
    dff = filter_data(selected_row_indices)
    return 'You\'ve selected rows' #shorttable(rows,selected_row_indices)
    #either:
    #selected_rows=[rows[i] for i in selected_row_indices]
    #or
    #selected_rows=pd.DataFrame(rows).iloc[i] 
    #return do_something



@app.callback(dash.dependencies.Output('output_state', "children"),[dash.dependencies.Input('submit_button', 'n_clicks')],[dash.dependencies.State('city_input', 'value'),dash.dependencies.State('language_input', 'value')])
def update_output(n_clicks,city_input, language_input):
    data1=get_candidate_info(language_input,city_input)
    size =data1.size    
    DF_ghstat=data1;

    data_rec = data1.to_dict('records')
    mycolumns = [{'name': i, 'id': i} for i in data1.columns]
                
    return html.Div( style= {'margin':'5px 10%'}, children=[
                dash_table.DataTable(
            id='datatable-interactivity',
            columns= mycolumns,
            data=data1.to_dict("rows"),
            editable=True,
            filter_action="native",
            sort_action="native",
            sort_mode="multi",
            column_selectable="single",
            row_selectable="single",
            row_deletable=True,
            selected_columns=[],
            selected_rows=[],
            page_action="native",
            page_current= 0,
            page_size= 10,
            style_cell={
                        'fontSize':20, 'font-family':'sans-serif',
                        # all three widths are needed
                        'minWidth': '170px', 'width': '100px', 'maxWidth': '180px',
                        'overflow': 'hidden',
                        'textOverflow': 'ellipsis',
            },
            style_cell_conditional=[
            {
                'if': {'column_id': c},
                    'display': 'none'
            } for c in ['proj_owned','commit','issue']
            ]
         ),
        
               
        html.Div(id='userselected-div')
        ]
        
        )
    return u' Input 1 is "{}" and Input 2 is "{}", got user data{}'.format(city_input, language_input,size)




#-----------------------------------------------------------------






def get_Host_name_IP(): 
    host_ip=""
    try: 
        host_name = socket.gethostname() 
        host_ip = socket.gethostbyname(host_name) 
        print("Hostname :  ",host_name) 
        print("IP : ",host_ip) 
    except: 
        print("Unable to get Hostname and IP") 
    return host_ip





if __name__ == '__main__':
    app.run_server(debug=False, host='0.0.0.0')
   #app.run_server(debug=False, host='172.31.24.153', port=5000)
    #app.run_server(debug=False, host='100.20.116.130', port=5040)

