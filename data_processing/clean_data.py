import pandas as pd
import numpy as np
import zipfile
from io import BytesIO
from s3_functions import read_s3_obj, put_csv_to_s3
import io


def make_angle(x, y):
    x_area = 100 * 105 / 100
    y_area_low = 37 * 68 / 100
    y_area_up = 63 * 68 / 100

    b = np.abs((y_area_low - y_area_up))
    a = np.sqrt((x - x_area) ** 2 + (y - y_area_low) ** 2)
    c = np.sqrt((x - x_area) ** 2 + (y - y_area_up) ** 2)
    cos = -1 / (2 * a * c) * (b ** 2 - a ** 2 - c ** 2)

    return np.arccos(cos)


def make_distance(x, y):
    x_goal = 100 * 105 / 100
    y_goal = 50 * 68 / 100
    return np.sqrt((x - x_goal) ** 2 + (y - y_goal) ** 2)


def make_shot_df(event, tag_names):
    dt_shots = event[event['eventName'] == 'Shot']
    dt_player = dt_shots[['id', 'playerId']]
    dt_shots = dt_shots.explode('tags', ignore_index=True)
    # Convert tags to a value
    dt_shots['tags'] = dt_shots['tags'].apply(pd.Series)
    dt_shots = dt_shots.explode('positions')
    dt_shots['dummy'] = np.tile(['start', 'end'], dt_shots.shape[0] // 2)
    dt_shots = pd.concat([
        dt_shots.drop(['positions', 'dummy'], axis=1),
        dt_shots.pivot(columns='dummy', values='positions')
    ], axis=1)
    dt_shots['start_x'] = pd.Series([x['x'] for x in dt_shots.start])
    dt_shots['start_y'] = pd.Series([x['y'] for x in dt_shots.start])
    dt_shots['end_x'] = pd.Series([x['x'] for x in dt_shots.end])
    dt_shots['end_y'] = pd.Series([x['y'] for x in dt_shots.end])
    dt_shots = dt_shots.drop(['start', 'end'], axis=1).drop_duplicates(ignore_index=True)
    # Convert (x,y) from percentage of pitch to coordinates (pitch is 105x68 meters)
    dt_shots[['start_x', 'end_x']] = dt_shots[['start_x', 'end_x']].apply(lambda x: x * 105.0 / 100.0)
    dt_shots[['start_y', 'end_y']] = dt_shots[['start_y', 'end_y']].apply(lambda x: x * 68.0 / 100.0)
    dt_shots = dt_shots.merge(tag_names, on='tags', how='left').drop(['tags'], axis=1)

    dt_shots = dt_shots.drop(
        [
            'eventId', 'subEventName', 'playerId',
            'matchId', 'eventName', 'teamId', 'matchPeriod',
            'eventSec', 'subEventId'
        ],
        axis=1
    )

    dt_shots = pd.get_dummies(dt_shots, columns=['label'])
    dt_positions = dt_shots[['id', 'start_x', 'start_y']]. \
        groupby('id'). \
        agg('mean'). \
        reset_index()

    dt_labels = dt_shots. \
        drop(['start_x', 'start_y', 'end_x', 'end_y'], axis=1). \
        groupby('id'). \
        agg('sum'). \
        reset_index()

    dt_shots_clean = dt_positions.merge(dt_labels, on='id', how='left')
    dt_shots_clean = dt_shots_clean.merge(dt_player, on='id', how='left')
    dt_shots_clean['angle'] = make_angle(dt_shots_clean.start_x, dt_shots_clean.start_y)
    dt_shots_clean['distance'] = make_distance(dt_shots_clean.start_x, dt_shots_clean.start_y)

    return dt_shots_clean


def make_comp_data(event, filename):
    event = event[['id', 'teamId', 'eventName']]
    event = event[event['eventName'] == 'Shot']
    event = event.drop(['eventName'], axis=1)
    competition = filename.replace('.json', '').replace('events_', '')
    event['competition'] = competition

    return event


def read_data():
    # tags data
    response_body = read_s3_obj('raw-data/tags2name.csv', 'tags')
    tags = pd.read_csv(response_body).drop(
        ['Description'],
        axis=1
    ).rename(
        columns={'Tag': 'tags', 'Label': 'label'}
    )

    # events data
    response_body = read_s3_obj('raw-data/events.zip', 'events')
    shots = pd.DataFrame()
    id_comp = pd.DataFrame()

    with io.BytesIO(response_body.read()) as tf:
        # rewind the file
        tf.seek(0)

        # Read the file as a zipfile and process the members
        with zipfile.ZipFile(tf, mode='r') as zipf:
            for filename in zipf.namelist():
                print(f'Importing {filename}')
                with zipf.open(filename) as file:
                    event = pd.read_json(BytesIO(file.read()))
                    shots_comp = make_shot_df(event, tags)
                    shots = pd.concat([shots, shots_comp])

                    id_event = make_comp_data(event, filename)
                    id_comp = pd.concat([id_comp, id_event])

    # players data
    response_body = read_s3_obj('raw-data/players.json', 'players')
    with io.BytesIO(response_body.read()) as bio:
        # with open(response_body, encoding="unicode_escape") as f:
        players = pd.read_json(bio, encoding="unicode_escape")


    # teams data
    response_body = read_s3_obj('raw-data/teams.json', 'teams')
    # with open(response_body, encoding="unicode_escape") as f:
    with io.BytesIO(response_body.read()) as bio:
        teams = pd.read_json(bio, encoding="unicode_escape")
        teams = teams[['name', 'wyId']]
        teams = teams.rename(columns={'wyId': 'teamId', 'name': 'teamName'})

    return [shots, id_comp, players, teams]


def main():
    df_shots, teams, df_players, teams_id = read_data()
    print('Data imported')

    # add shots data
    df_shots['start_x'] = df_shots['start_x'] / 105 * 120
    df_shots['start_y'] = df_shots['start_y'] / 68 * 90
    print('Created shots data')

    # add competition info
    df_shots = df_shots.merge(teams, on='id', how='left')
    df_shots = df_shots.loc[
        (df_shots['competition'] != 'European_Championship') & (df_shots['competition'] != 'World_Cup')
        ]
    print('Merged competition data')

    # add player info 
    df_players = pd.concat(
        [df_players.loc[:, ['weight', 'shortName', 'firstName', 'lastName', 'birthDate', 'height', 'wyId', 'foot']],
         pd.DataFrame(df_players['role'].tolist()).loc[:, 'name']], axis=1). \
        rename(columns={'wyId': 'playerId', 'name': 'position'})

    df_shots = df_shots.merge(df_players, on='playerId')
    print('Merged players data')

    # add team info 
    df_shots = df_shots.merge(teams_id, on='teamId', how='left')
    print('Merged teams data')

    # write processed data to s3
    put_csv_to_s3(df_shots, 'processed-data/processed_shots.csv', 'processed shots')

    return


if __name__ == "__main__":
    main()
