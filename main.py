import base64
import os
from google.cloud import bigquery
from google.cloud import storage
import pandas as pd
import numpy as np
from collections import defaultdict
from datetime import date, timedelta

bq_table_location = "project-id.dataset-id.table-id" #Where the path data is 
cookieCol = "clientId"
#Paths (each row) must be in the string format [Start, a, b, c,..., NULL] 
pathCol = "Custom_Channel_Grouping" #Name of path coloumn to use.


def markov_attribution(event, context):
     """Triggered from a message on a Cloud Pub/Sub topic.
     Args:
          event (dict): Event payload.
          context (google.cloud.functions.Context): Metadata for the event.
     """
     pubsub_message = base64.b64decode(event['data']).decode('utf-8')
     print(pubsub_message)

    
     #Authenticte
     #credential_path = "nvh-bigquery-export-09bf57297b37.json"
     #os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path


     def get_data_bq(table_location): #assumes authentication and a scheduled query set up
          bq_client = bigquery.Client()
          QUERY = "SELECT * FROM `"+table_location+"`"
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
          channels = [channel for channel in df.columns if channel not in ['Start','Null','Conversion']]
          for channel in channels:
               removal_df = df.drop(channel, axis=1).drop(channel, axis=0)
               for column in removal_df.columns:
                    row_sum = np.sum(list(removal_df.loc[column]))
                    null_pct = float(1) - row_sum
                    if null_pct != 0:
                         removal_df.loc[column]['Null'] = null_pct
                    removal_df.loc['Null']['Null'] = 1.0

               removal_to_conv = removal_df[['Null', 'Conversion']].drop(['Null', 'Conversion'], axis=0)
               removal_to_non_conv = removal_df.drop(['Null', 'Conversion'], axis=1).drop(['Null', 'Conversion'], axis=0)

               removal_inv_diff = np.linalg.inv(np.identity(len(removal_to_non_conv.columns)) - np.asarray(removal_to_non_conv))
               removal_dot_prod = np.dot(removal_inv_diff, np.asarray(removal_to_conv))
               removal_cvr = pd.DataFrame(removal_dot_prod,index=removal_to_conv.index)[[1]].loc['Start'].values[0]
               removal_effect = 1 - removal_cvr / conversion_rate
               removal_effects_dict[channel] = removal_effect

          return removal_effects_dict

     def markov_chain_allocations(removal_effects, total_conversions):
          re_sum = np.sum(list(removal_effects.values()))

          return {k: (v / re_sum) * total_conversions for k, v in removal_effects.items()}


     # Get and ready the data
     #df_paths = pd.read_csv("markov_path.csv")
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
     startWeek = yesterday - timedelta(days=yesterday.weekday())
     
     yesterday = yesterday.strftime("%Y-%m-%d")
     attributions["end_of_week"] = yesterday

     startWeek = startWeek.strftime("%Y-%m-%d")
     attributions["start_of_week"] = startWeek

     print(attributions)

     #Send data to Bigquery 
     attributions.to_gbq("attribution_path_workflow.nvh_attributed_conversion", project_id="nvh-bigquery-export", if_exists='append')



