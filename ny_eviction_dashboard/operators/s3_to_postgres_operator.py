from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook

import json
import io
from contextlib import closing

class S3ToPostgresOperator(BaseOperator):
	"""
	Collects data from a file hosted on AWS S3 and loads it into a Postgres table.
	Current version supports JSON and CSV sources but requires pre-defined data model. 

	:param s3_conn_id:       S3 connection id
	:param s3_bucket:        S3 Bucket Destination
	:param s3_prefix:        S3 File Prefix
	:param source_data_type: S3 Source File data type
	:param header:           Toggles ignore header for CSV source type
	:param postgres_conn_id: Posgres Connection id
	:param db_schema:        Postgres Target Schema
	:param db_table:         Postgres Target Table
	:param get_latest:       if True, pulls from last modified file in S3 path
	"""

	@apply_defaults
	def __init__(self,
		s3_conn_id=None,
		s3_bucket=None,
		s3_prefix='',
		source_data_type='',
		postgres_conn_id='postgres_default',
		header=False,
		schema='public',
		table='raw_load',
		get_latest=False,		
		*args, 
		**kwargs) -> None: 
		super().__init__(*args, **kwargs)
		
		self.s3_conn_id = s3_conn_id
		self.s3_bucket = s3_bucket
		self.s3_prefix = s3_prefix
		self.source_data_type = source_data_type
		self.postgres_conn_id = postgres_conn_id
		self.header = header
		self.schema = schema
		self.table = table
		self.get_latest = get_latest

	def execute(self, context):
		"""
		Executes the operator.
		"""
		s3_hook = S3Hook(self.s3_conn_id)
		s3_session = s3_hook.get_session()
		s3_client = s3_session.client('s3')

		if self.get_latest == True:
			objects = s3_client.list_objects_v2(Bucket=self.s3_bucket, Prefix=self.s3_prefix)['Contents']
			latest = max(objects, key=lambda x: x['LastModified'])
			s3_obj = s3_client.get_object(Bucket=self.s3_bucket, Key=latest['Key'])

		file_content = s3_obj['Body'].read().decode('utf-8')
		pg_hook = PostgresHook(self.postgres_conn_id)

		if self.source_data_type == 'json':
			print('inserting json object...')

			json_content = json.loads(file_content)

			schema = self.schema
			if isinstance(self.schema, tuple):
				schema = self.schema[0]
			
			table = self.table
			if isinstance(self.table, tuple):
				table = self.table[0]
			
			target_fields = ['court_index_number', 
											'docket_number', 
											'eviction_address',
											'executed_date',
											'residential_commercial_ind',
											'borough',
											'eviction_zip',
											'ejectment',
											'eviction_possession',
											'latitude',
											'longitude',
											'community_board',
											'council_district',
											'census_tract',
											'bin',
											'bbl',
											'nta']
		target_fields = ','.join(target_fields)

		with closing(pg_hook.get_conn()) as conn:
			with closing(conn.cursor()) as cur:
				cur.executemany(
					f"""INSERT INTO {schema}.{table} ({target_fields})
					VALUES(
					%(court_index_number)s, %(docket_number)s, %(eviction_address)s, 
					%(executed_date)s, %(residential_commercial_ind)s, %(borough)s,
					%(eviction_zip)s, %(ejectment)s, %(eviction_possession)s, %(latitude)s, 
					%(longitude)s, %(community_board)s, %(council_district)s,
					%(census_tract)s, %(bin)s, %(bbl)s, %(nta)s
					);
					""",
					({
						'court_index_number': line['court_index_number'], 
						'docket_number': line['docket_number'], 
						'eviction_address': line['eviction_address'], 
						'executed_date': line['executed_date'], 
						'residential_commercial_ind': line['residential_commercial_ind'], 
						'borough': line['borough'],
						'eviction_zip': line['eviction_zip'], 
						'ejectment': line['ejectment'], 
						'eviction_possession': line['eviction_possession'], 
						'latitude': line.get('latitude', None), 
						'longitude': line.get('longitude', None), 
						'community_board': line.get('community_board', None), 
						'council_district': line.get('council_district', None), 
						'census_tract': line.get('census_tract', None),
						'bin': line.get('bin', None),
						'bbl': line.get('bbl', None),
						'nta': line.get('nta', None)
						} for line in json_content))
				conn.commit()
		if self.source_data_type == 'csv':
			
			print('inserting csv...')
			file = io.StringIO(file_content)

			sql = "COPY %s FROM stdin DELIMITER ','"
			if self.header == True:
				sql = "COPY %s FROM STDIN DELIMITER ',' CSV HEADER"

			schema = self.schema
			if isinstance(self.schema, tuple):
				schema = self.schema[0]
			
			table = self.table	
			if isinstance(self.table, tuple):
				table = self.table[0]	
				
			table = f'{schema}.{table}'	
			
			with closing(pg_hook.get_conn()) as conn:
				with closing(conn.cursor()) as cur:
					cur.copy_expert(sql=sql % table, file=file)
					conn.commit()
	
	print('inserting complete...')
