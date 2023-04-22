import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from s3_functions import read_s3_obj, put_csv_to_s3
import io


# read in model data from s3
response_body = read_s3_obj('processed-data/processed_shots.csv', 'processed shots')
csv_string = response_body.read().decode('unicode_escape')
dt_shots = pd.read_csv(io.StringIO(csv_string))

# run model
# Select relevant features
dt_shots = dt_shots[[
    'start_x', 'start_y', 'label_Left', 'label_Right', 'label_counter_attack',
    'label_head/body', 'label_interception', 'angle', 'distance', 'label_Goal'
]].dropna()

# Exclude shots taken from too far away
dt_shots = dt_shots[dt_shots['start_x'] > 70 * 105.0 / 100.0]

# Train-test split
X_train, X_test, y_train, y_test = train_test_split(
    dt_shots.drop('label_Goal', axis=1), dt_shots.label_Goal, test_size=0.2,
    random_state=42,
    stratify=dt_shots.label_Goal
)

# Final model after cross-validation
final_rf = RandomForestClassifier(
    criterion='entropy',
    n_estimators=1000,
    max_features=9,
    n_jobs=2,
    min_samples_split=500,
    random_state=42,
    verbose=0
)

final_rf_fit = final_rf.fit(X_train, y_train)
print('Finish training model')


# make players predictions
rf_columns = [
    'start_x', 'start_y', 'label_Left', 'label_Right', 'label_counter_attack',
    'label_head/body', 'label_interception', 'angle', 'distance'
]

response_body = read_s3_obj('processed-data/processed_shots.csv', 'processed shots')
csv_string = response_body.read().decode('unicode_escape')
df_full = pd.read_csv(io.StringIO(csv_string)).dropna()
df_full['xg'] = final_rf_fit.predict_proba(df_full[rf_columns])[:, 1]

df_players_xg = df_full[['shortName', 'label_Goal', 'xg']].\
    groupby('shortName', as_index=False).\
    agg('sum').\
    sort_values('xg', ascending=False)

df_players_xg['diff'] = df_players_xg['label_Goal'] - df_players_xg['xg']

# save model results in s3
put_csv_to_s3(df_players_xg, 'processed-data/model_res_players.csv', 'model players results')
print('Finish making players predictions')


# make teams predictions
df_teams_xg = df_full[['label_Goal', 'xg', 'teamId']].\
    groupby('teamId', as_index=False).\
    agg('sum')

df_teams_xg['diff'] = df_teams_xg['label_Goal'] - df_teams_xg['xg']

# store model results in s3
put_csv_to_s3(df_teams_xg, 'processed-data/model_res_teams.csv', 'model team results')
print('Finish making teams predictions')