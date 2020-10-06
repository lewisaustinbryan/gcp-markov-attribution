import os
from google.cloud import bigquery
from google.cloud import storage
import pandas as pd
import numpy as np
from collections import defaultdict
from datetime import date, timedelta
from dateutil.rrule import rrule, WEEKLY


bq_table_location = "nvh-bigquery-export.attribution_path_workflow.nvh_markov_paths_backdate"
cookieCol = "clientId"
#Paths (each row) must be in the string format [Start, a, b, c,..., NULL] 
pathCol = "Custom_Channel_Grouping"


credential_path = "nvh-bigquery-export-09bf57297b37.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path

def markov_attribution(backdate = True):

	def get_data_bq(table_location): #assumes authentication 
		bq_client = bigquery.Client()
		QUERY = "SELECT * FROM `"+table_location+"`"
		query_job = bq_client.query(QUERY) # API request
		return query_job.result().to_dataframe() # Waits for query to finish	

	def get_data_bq_backdate(start_date, end_date): #assumes authentication
		bq_client = bigquery.Client()
		QUERY = '''
			CREATE TEMP FUNCTION first_element_of_array(arr ANY TYPE) AS (
			  arr[offset(0)]
			);
			CREATE TEMP FUNCTION last_element_of_array(arr ANY TYPE) AS (
			  arr[ORDINAL(ARRAY_LENGTH(arr))]
			);
			#SELECT  Custom_Channel_Grouping, SUM(conversion) FROM (
			WITH markov_ready_unnest AS(
			    SELECT
			      clientId,
			      visitNumber,
			      Date,
			    CASE 
			      WHEN 
			        source = "(direct)" 
			        AND 
			        (medium = "(not set)" OR medium = "(none)") 
			        THEN "Direct"
			      WHEN 
			        medium = "organic"
			        THEN "Organic Search"
			      WHEN (
			          CASE
			              WHEN
			                  COUNTIF(hasSocialSourceReferral = "Yes") > 0
			            THEN "Yes"
			            ELSE "No"
			         END
			        ) = "Yes"
			        OR
			        REGEXP_CONTAINS(medium, r"^(social|social-network|social-media|sm|social network|social media)$")
			        THEN "Social"
			      WHEN 
			        medium = "paid-social"
			        THEN "Paid Social" 
			      WHEN
			        medium = "email"
			        THEN "Email"
			      WHEN medium = "affiliate" THEN "Affiliates"
			      WHEN
			        medium = "referral"
			        THEN "Referral"
			      WHEN
			        REGEXP_CONTAINS(medium, r"^(cpc|ppc|paidsearch)$")
			        AND     
			        adNetworkType <> "Content"
			        AND REGEXP_CONTAINS(campaign, r"Brand")
			        THEN "Paid Search Brand"
			      WHEN
			        REGEXP_CONTAINS(medium, r"^(cpc|ppc|paidsearch)$")
			        AND     
			        adNetworkType <> "Content"
			        AND REGEXP_CONTAINS(campaign, r"RLSA")
			        THEN "Paid Search RLSA"
			      WHEN
			        REGEXP_CONTAINS(medium, r"^(cpc|ppc|paidsearch)$")
			        AND     
			        adNetworkType <> "Content"
			        THEN "Paid Search Non-Brand"
			      WHEN
			        REGEXP_CONTAINS(medium, r" ^(cpv|cpa|cpp|content-text)$")
			        THEN "Other Advertising"
			      WHEN
			        REGEXP_CONTAINS(medium, r"^(display|cpm|banner)$")
			        OR      
			        adNetworkType = "Content"
			        THEN "Display"
			      ELSE "(Other)"
			  END
			 as Custom_Channel_Grouping,
			  SUM(conversion) AS conversion,

			    FROM (
			      SELECT  
			        clientId,
			        visitNumber,
			        PARSE_DATE('%Y%m%d', date) as Date,


			        IF (trafficSource.isTrueDirect, "(direct)", trafficSource.source) AS source,
			        IF (trafficSource.isTrueDirect, "(none)", trafficSource.medium) AS medium,
			        IF (trafficSource.isTrueDirect, "(not set)", trafficSource.campaign) AS campaign,
			        IF (trafficSource.isTrueDirect, "(not set)", trafficSource.keyword) AS keyword,
			        IF (trafficSource.isTrueDirect,"Direct", channelGrouping) AS channelGrouping,
			        trafficSource.adwordsClickInfo.adNetworkType AS adNetworkType,
			        social.hasSocialSourceReferral AS hasSocialSourceReferral,
			      CASE 
			        WHEN 
			          hits.eventInfo.eventCategory = 'Phone call' THEN COUNT(DISTINCT CONCAT(CAST(fullVisitorId AS STRING), CAST(visitStartTime AS STRING))) 
			        WHEN hits.eventInfo.eventCategory = 'Enquiry' AND hits.eventInfo.eventAction = 'Form Submit' THEN COUNT(DISTINCT CONCAT(CAST(fullVisitorId AS STRING), CAST(visitStartTime AS STRING))) 
			        WHEN hits.eventInfo.eventCategory = 'SmartSupp' AND hits.eventInfo.eventAction = 'Conversation' THEN COUNT(DISTINCT CONCAT(CAST(fullVisitorId AS STRING), CAST(visitStartTime AS STRING))) 
			        WHEN hits.eventInfo.eventCategory = 'ResponseiQ' AND hits.eventInfo.eventAction = 'Contact' THEN COUNT(DISTINCT CONCAT(CAST(fullVisitorId AS STRING), CAST(visitStartTime AS STRING))) 
			        ELSE 0 
			      END AS conversion
			     FROM `nvh-bigquery-export.178796018.ga_sessions_*`AS table, 
			                                 UNNEST( hits ) as hits
			                                  
			                                  WHERE PARSE_DATE("%Y%m%d", date) BETWEEN 
			                                    DATE_sub("'''+end_date+'''", interval 360 day) AND "'''+end_date+'''"
			                                    AND totals.visits = 1
			      GROUP BY
			        clientId, visitNumber, Date, channelGrouping, source, medium, campaign, keyword, adNetworkType, hasSocialSourceReferral ,
			        campaign, medium, source, keyword, adNetworkType, hits.eventInfo.eventCategory, hits.eventInfo.eventAction
			      )
			      GROUP BY
			      clientId, visitNumber, Date,
			    campaign, medium, source, keyword, adNetworkType
			#) GROUP BY Custom_Channel_Grouping
			),
			markov_ready_array AS (

			-- GET INDIVIDUAL USER JOURNIES:
			---- (OPTIONAL) APPLY SEGMENTATION
			---- REMOVE USERS WHOES:- 
			------  FINAL END DATE IS LOWER THAN UI START DATE
			------  (OPTIONAL) FIRST VISITNUMBER IS NOT 1 (TRUE FIRST CLICK) 
			SELECT 
			  clientId,
			  Date_arr, visitNumber_arr, Custom_Channel_Grouping_arr,
			  ARRAY_TO_STRING(Custom_Channel_Grouping_arr , ", ") AS funnel,
			  CASE 
			    WHEN first_element_of_array(visitNumber_arr) = 1 THEN true
			    ELSE false
			  END AS is_true_first_click,
			  conversion_lookback,conversion_from_start_date,	
			  CASE
			    WHEN conversion_from_start_date = 0 THEN 1 
			    ELSE 0
			  END AS no_of_nulls
			FROM(
			  -- AGGREGATE DATE, VISITNUMBER AND DIMENSIION INTO ARRAY
			    SELECT 
			      clientId,
			      ARRAY_AGG(Date ORDER BY Date) AS Date_arr,
			      ARRAY_AGG(visitNumber ORDER BY visitNumber) as visitNumber_arr,
			      ARRAY_AGG(Custom_Channel_Grouping ORDER BY Date, visitNumber) AS Custom_Channel_Grouping_arr,
			      SUM(conversion) as conversion_lookback,SUM(conversion_from_start_date) as conversion_from_start_date
			    FROM ( 
			    -- SEPARATE CONVERSION INTO LOOKBACK AND START DATE AGGRAGTION 
			      SELECT 
			        * ,
			CASE 
			WHEN UNIX_DATE(Date) < UNIX_DATE("'''+start_date+'''") THEN NULL 
			ELSE conversion
			END as conversion_from_start_date
			FROM markov_ready_unnest AS table 
			    )
			    GROUP BY clientId
			)
			WHERE 
			  --first_element_of_array(visitNumber_arr) = 1
			  --AND 
			  UNIX_DATE(last_element_of_array(Date_arr)) >= UNIX_DATE("'''+start_date+'''")

			)
			#SELECT * FROM markov_ready_unnest

			  --REMOVE ARRAYS AND AGGREGATE DIMENSION
			  SELECT 
			  #is_true_first_click,
			  clientId,
			  CASE
			    WHEN SUM(conversion_from_start_date) > 0 THEN CONCAT(CONCAT("[Start,"," ", funnel), ", Conversion]")
			    WHEN SUM(no_of_nulls) > 0 THEN CONCAT(CONCAT("[Start,"," ", funnel), ", Null]")
			  END AS Custom_Channel_Grouping
			  FROM  markov_ready_array
			  GROUP BY #is_true_first_click, 
			    clientId, funnel
			  #ORDER BY conversion DESC
		'''
		#print(QUERY)
		query_job = bq_client.query(QUERY) # API request
		return query_job.result().to_dataframe() # Waits for query to finish	

	def transition_states(list_of_paths):
		list_of_unique_channels = set(x for element in list_of_paths for x in element)
		transition_states = {x + '>' + y: 0 for x in list_of_unique_channels for y in list_of_unique_channels}

		for possible_state in list_of_unique_channels:
			if possible_state not in ['Conversion', 'Null']:
				for user_path in list_of_paths:
					if possible_state in user_path:
						indices = [i for i, s in enumerate(user_path) if possible_state in s]
						for col in indices:
							transition_states[user_path[col] + '>' + user_path[col + 1]] += 1
    	
		return transition_states

	def transition_prob(trans_dict):
	    list_of_unique_channels = set(x for element in list_of_paths for x in element)
	    trans_prob = defaultdict(dict)
	    for state in list_of_unique_channels:
	        if state not in ['Conversion', 'Null']:
	            counter = 0
	            index = [i for i, s in enumerate(trans_dict) if state + '>' in s]
	            for col in index:
	                if trans_dict[list(trans_dict)[col]] > 0:
	                    counter += trans_dict[list(trans_dict)[col]]
	            for col in index:
	                if trans_dict[list(trans_dict)[col]] > 0:
	                    state_prob = float((trans_dict[list(trans_dict)[col]])) / float(counter)
	                    trans_prob[list(trans_dict)[col]] = state_prob

	    return trans_prob

	def transition_matrix(list_of_paths, transition_probabilities):
	    trans_matrix = pd.DataFrame()
	    list_of_unique_channels = set(x for element in list_of_paths for x in element)

	    for channel in list_of_unique_channels:
	        trans_matrix[channel] = 0.00
	        trans_matrix.loc[channel] = 0.00
	        trans_matrix.loc[channel][channel] = 1.0 if channel in ['Conversion', 'Null'] else 0.0

	    for key, value in transition_probabilities.items():
	        origin, destination = key.split('>')
	        trans_matrix.at[origin, destination] = value

	    return trans_matrix

	def removal_effects(df, conversion_rate):
	    removal_effects_dict = {}
	    channels = [channel for channel in df.columns if channel not in ['Start',
	                                                                     'Null',
	                                                                     'Conversion']]
	    for channel in channels:
	        removal_df = df.drop(channel, axis=1).drop(channel, axis=0)
	        for column in removal_df.columns:
	            row_sum = np.sum(list(removal_df.loc[column]))
	            null_pct = float(1) - row_sum
	            if null_pct != 0:
	                removal_df.loc[column]['Null'] = null_pct
	            removal_df.loc['Null']['Null'] = 1.0

	        removal_to_conv = removal_df[
	            ['Null', 'Conversion']].drop(['Null', 'Conversion'], axis=0)
	        removal_to_non_conv = removal_df.drop(
	            ['Null', 'Conversion'], axis=1).drop(['Null', 'Conversion'], axis=0)

	        removal_inv_diff = np.linalg.inv(
	            np.identity(
	                len(removal_to_non_conv.columns)) - np.asarray(removal_to_non_conv))
	        removal_dot_prod = np.dot(removal_inv_diff, np.asarray(removal_to_conv))
	        removal_cvr = pd.DataFrame(removal_dot_prod,
	                                   index=removal_to_conv.index)[[1]].loc['Start'].values[0]
	        removal_effect = 1 - removal_cvr / conversion_rate
	        removal_effects_dict[channel] = removal_effect

	    return removal_effects_dict

	def markov_chain_allocations(removal_effects, total_conversions):
	    re_sum = np.sum(list(removal_effects.values()))

	    return {k: (v / re_sum) * total_conversions for k, v in removal_effects.items()}

	# Get and ready the data
	#df_paths = pd.read_csv("markov_path.csv")
	if (backdate == False):
		df_paths = get_data_bq(bq_table_location)
		df_paths[pathCol] = df_paths.apply(lambda row: row[pathCol].strip('][').split(', '), axis = 1)
		print(df_paths)

		#each path in a list-->
		list_of_paths = df_paths[pathCol]
		print(list_of_paths)
		#We'll use these metrics to help form removal effect and attributed values-->
		total_conversions = sum(path.count('Conversion') for path in df_paths[pathCol].tolist())
		print(total_conversions)
		base_conversion_rate = total_conversions / len(list_of_paths)

		#Put data into markov state format
		trans_states = transition_states(list_of_paths)
		print(trans_states)

		#Get the probabilties form states 
		trans_prob = transition_prob(trans_states)
		print(trans_prob)

		#Form Transition Matrix
		trans_matrix = transition_matrix(list_of_paths, trans_prob)
		print(trans_matrix)

		#Get the removal effects from transition probablity matrix
		print(base_conversion_rate)
		removal_effects_dict = removal_effects(trans_matrix, base_conversion_rate)
		print(removal_effects_dict)

		#Use removal effects to calculate attributed values for channels
		attributions = markov_chain_allocations(removal_effects_dict, total_conversions)
		attributions = pd.DataFrame(list(attributions.items()),columns=["channel", "attributed_conversions"]) 
		yesterday = date.today() - timedelta(days=1)
		yesterday = yesterday.strftime("%Y-%m-%d")
		attributions["date"] = yesterday
		print(attributions)

		#Send data to Bigquery 
		return attributions.to_gbq("attribution_path_workflow.nvh_attributed_conversion", project_id="nvh-bigquery-export", if_exists='append')
	else:
		cookieCol = "clientId"
		pathCol = "Custom_Channel_Grouping"

		user_lyfecycle_days = 360/2
		lastDate = date(2018,7,20) + timedelta(days = user_lyfecycle_days)
		dayInLastWeek = date.today() - timedelta(days = 6)
		firstStartWeek = dayInLastWeek - timedelta(days=dayInLastWeek.weekday())
		finish = firstStartWeek + timedelta(days = 6)
		start = dayInLastWeek - timedelta(days=(finish - lastDate).days)

		for week in rrule(WEEKLY, dtstart=start, until=finish):
			startWeek = week - timedelta(days=week.weekday())
			endWeek = startWeek + timedelta(days = 6)
			startWeek = startWeek.strftime("%Y-%m-%d")
			endWeek = endWeek.strftime("%Y-%m-%d")
			print("Running BQ SQL from " + startWeek +" to "+endWeek )
			df_paths = get_data_bq_backdate(start_date = startWeek, end_date = endWeek)	
			#print(df_paths)
			df_paths[pathCol] = df_paths.apply(lambda row: row[pathCol].strip('][').split(', '), axis = 1)
			#print(df_paths)

			#each path in a list-->
			list_of_paths = df_paths[pathCol]
			#print(list_of_paths)
			#We'll use these metrics to help form removal effect and attributed values-->
			total_conversions = sum(path.count('Conversion') for path in df_paths[pathCol].tolist())
			#print(total_conversions)
			base_conversion_rate = total_conversions / len(list_of_paths)

			#Put data into markov state format
			trans_states = transition_states(list_of_paths)
			#print(trans_states)

			#Get the probabilties form states 
			trans_prob = transition_prob(trans_states)
			#print(trans_prob)

			#Form Transition Matrix
			trans_matrix = transition_matrix(list_of_paths, trans_prob)
			#print(trans_matrix)

			#Get the removal effects from transition probablity matrix
			#print(base_conversion_rate)
			removal_effects_dict = removal_effects(trans_matrix, base_conversion_rate)
			#print(removal_effects_dict)

			#Use removal effects to calculate attributed values for channels
			attributions = markov_chain_allocations(removal_effects_dict, total_conversions)
			attributions = pd.DataFrame(list(attributions.items()),columns=["channel", "attributed_conversions"]) 
			
			attributions["start_of_week"] = startWeek
			attributions["end_of_week"] = endWeek
			print(attributions)

			attributions.to_gbq("attribution_path_workflow.nvh_attributed_conversion", 
				project_id="nvh-bigquery-export",
				if_exists='append',
				table_schema = [
					{'name': "channel", 'type': 'STRING'},
					{'name': "attributed_conversions", 'type': 'FLOAT'},
					{'name': "start_of_week", 'type': 'STRING'},
					{'name': "end_of_week", 'type': 'STRING'}
				]
			)

markov_attribution()


