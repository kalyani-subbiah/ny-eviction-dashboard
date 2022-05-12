from dash import Dash, html, dcc
import plotly.express as px
import psycopg2
import pandas as pd
import geopandas as gpd

app = Dash(__name__)

geodf = gpd.read_file('path_to_shp_file')

def generate_table(dataframe, max_rows=10):
	return html.Table([
		html.Thead(
			html.Tr([html.Th(col) for col in dataframe.columns])
		),
		html.Tbody([
			html.Tr([
				html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
			]) for i in range(min(len(dataframe), max_rows))
		])
	])

def load_df():
	conn = psycopg2.connect("dbname=evictions user=evictions")
	cur = conn.cursor()
	cur.execute("SELECT * FROM raw.soda_evictions")
	colnames = [desc[0] for desc in cur.description]
	records = cur.fetchall()
	records = pd.DataFrame(records)
	records.columns = colnames
	return records

def borough_counts(df):
	borough_counts = df['borough'].value_counts()
	borough_counts = borough_counts.reset_index()
	borough_counts.columns = ['borough', 'count']
	return borough_counts

df = load_df()
df = borough_counts(df)

fig = px.bar(df, x="borough", y="count")


app.layout = html.Div(children=[
	html.H1(
		children='Evictions by borough',
		style={
			'textAlign': 'center',
		}
	),

	html.Div(
		children=
		'''
		Total evictions by borough
		''',
		style={
			'textAlign': 'center'
		}
	),
	dcc.Graph(id='example-graph',
	figure=fig),

	html.Div([
		html.H4(children='US Agriculture exports (2011)'),
		generate_table(df)
	])
])

if __name__ == '__main__':
	app.run_server(debug=True)