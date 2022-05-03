import pandas as pd

pd.set_option("display.max_columns", None)

def generate_anonymized_data():
    """
	Function that generates more anonymized data starting from a base ridm file already anonymized adding characters 'ABC' then combine it
    for times defined 

    TODO A stop at wanted filesize should be implemented
    """ 
    times = int(input("Please enter how many times you want to enlarge the datasets:\n"))
    print(f"You choosed to enlarge by {times} times")

    # extracting policies
    
    for x in range(times):
        policies = pd.read_csv("/Users/ludovicocesaro/Downloads/test/0/policies.csv")
        claims = pd.read_csv("/Users/ludovicocesaro/Downloads/test/0/claims.csv")
        persons = pd.read_csv("/Users/ludovicocesaro/Downloads/test/0/persons.csv")
        index = 281533
        # Creating new claims, persons , policies primary keys
        new_claims = claims.copy()
        new_claims['ID'] = new_claims['ID'] + 'ABC'
        new_persons = persons.copy()
        new_persons['ID'] = new_persons['ID'] + 'ABC'
        new_policies = policies.copy()
        new_policies['ID'] = new_policies['ID'] + 'ABC'

        # Taking random ids from persons and policies to make them match with claims foreign keys
        random_persons = new_persons.sample(n = round(index*(x+1)))
        random_persons.reset_index(inplace=True)

        random_policies = new_policies.sample(n = round(index*(x+1)))
        random_policies.reset_index(inplace=True)

        # Replacing correct keys to new generated claims
        new_claims['PERSON_ID'] = random_persons['ID']
        new_claims['POLICY_ID'] = random_policies['ID']

        claims_frames = [claims, new_claims] # New claims and old claims merged
        new_matched_claims = pd.concat(claims_frames)
        new_matched_claims.reset_index(inplace=True)
        new_matched_claims.drop(['index'], axis=1, inplace=True)
        print(new_matched_claims)

        persons_frames = [persons, new_persons]
        concat_persons = pd.concat(persons_frames)

        policies_frames = [policies, new_policies]
        concat_policies = pd.concat(policies_frames)

        new_matched_claims.to_csv("/Users/ludovicocesaro/Downloads/test/0/claims.csv")
        concat_persons.to_csv("/Users/ludovicocesaro/Downloads/test/0/persons.csv")
        concat_policies.to_csv("/Users/ludovicocesaro/Downloads/test/0/policies.csv")
        # Needs column fix

def fix_size():
    print('Askinf how much should be the size and reducing to')

generate_anonymized_data()