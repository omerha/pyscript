import argparse
import boto3
import json

from boto3.dynamodb.conditions import Key, Attr

temp_json_path = 'data/users.json'
DYNAMODB = boto3.resource('dynamodb', region_name='us-east-1')
CLIENT_SERVER_API_ERROR_DEV_TABLE = DYNAMODB.Table('via-client-server-api-error-dev')

def update_users_json_and_table(city_ids,should_delete = False):
    city_name_list = city_ids.split(",")
    with open(temp_json_path) as data_file:
        users_json = json.load(data_file)

    for city_name in city_name_list:
        city_user = city_name.split(":")
        update_users_json_helper(city_user[0].upper(), city_user[1], users_json, should_delete)

    with open(temp_json_path, 'w') as f:
        json.dump(users_json, f, indent=4)


def update_users_json_helper(city_id, slack_user_id, json_to_update, should_delete=False):
    slack_users_id_in_city = next((item['slack_user_id'] for item in json_to_update if item['city'] == city_id), None)
    if slack_users_id_in_city is not None:
        if slack_user_id in slack_users_id_in_city:
            if should_delete:
                slack_users_id_in_city.remove(slack_user_id)
        else:
            slack_users_id_in_city.append(slack_user_id)    
        print("Added new id {slack_user_id} to city {city_id}".format(slack_user_id=slack_user_id, city_id=city_id))

    else:
        item = {'city': city_id, 'slack_user_id': [slack_user_id]}
        json_to_update.append(item)
        print("Added new city {city_id}".format(city_id=city_id))

 #   put_item_in_table(city_id,slack_users_id_in_city)


def update_table_with_entire_json():
    with open(temp_json_path) as data_file:
        users_json = json.load(data_file)

    for item in users_json:
        print("updating city: {city}".format(city=item['city']))
        put_item_in_table(item['city'],item['slack_user_id'])
         
 
def main():
    parser = argparse.ArgumentParser(description='This script is responsible to update dynamodb table which connects \
    between slack user id and city')
    parser.add_argument('--load_table', help="loads the entire json into the table",action="store_true")
    parser.add_argument('--add_id', help="adds slack id to the right city, if the id exists it doesn't replicate,\
    also if the city doesn't exist it adds it\n for example --add_id NYC:3FUBGSZX,LON:5XCZVZXCV")
    parser.add_argument('--remove_id',help="Same as add_id, just for removing id")
    args = vars(parser.parse_args())
    if args["add_id"]:
        update_users_json_and_table(args["add_id"])
    elif args["load_table"]:
        update_table_with_entire_json()
    elif args["remove_id"]:
        update_users_json_and_table(args["remove_id"],True)


def put_item_in_table(city_key_table,slack_user_id_to_add):
    response = CLIENT_SERVER_API_ERROR_DEV_TABLE.put_item(
     Item={
        'city': city_key_table,
        'slack_user_id': slack_user_id_to_add
    })
    print("PutItem succeeded:")

if __name__ == '__main__':
    main()
