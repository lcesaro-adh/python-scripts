import pandas as pd
import subprocess

pd.set_option("display.max_columns", None)


def generate_anonymized_data():
    # Asks how big should be increased from ridm start in times
    times = int(input("Please enter how many times you want to enlarge the datasets:\n"))
    print(f"You choosed to enlarge by {times} times")

    for x in range(times):
        print('anonymization run', x)
        # RUN ANONYMIZATION SCRIPT
        # home_dir = subprocess.run(
        #     [
        #         "cd",
        #         "/Users/ludovicocesaro/Desktop/Files/Reply/Allianz/Projects/datahub-pipelines/tasks/common/anonymization",
        #     ]
        # )
        # python_env = subprocess.run(["pyenv", "activate", "dp"])
        # env_var = subprocess.run(["export", "PYTHONPATH=$PWD:$PYTHONPATH"])
        # print("Setup successful:", home_dir.returncode)
        # anonymize = subprocess.run(
        #     [
        #         f"python",
        #         "/Users/ludovicocesaro/Desktop/Files/Reply/Allianz/Projects/datahub-pipelines/tasks/common/anonymization/secure_anonymize.py",
        #         "/Users/ludovicocesaro/Downloads/anonymize",
        #         "/Users/ludovicocesaro/Downloads/test/{x}",
        #     ]
        # )  # command for anonymization
        # TODO: path must be generalized for working in all environments
        # TODO: FIX python_env & env_var / make work command
        print('')

def aggregate():
    # Test Data Generation code to copy from jupyter lab
    old_policies = pd.read_csv("/Users/ludovicocesaro/Downloads/anonymize/policies.csv", low_memory=False)
    new_policies = pd.read_csv("/Users/ludovicocesaro/Downloads/test/policies.csv", low_memory=False)

    old_persons = pd.read_csv("/Users/ludovicocesaro/Downloads/test/persons.csv", low_memory=False)
    new_persons = pd.read_csv("/Users/ludovicocesaro/Downloads/test/2/persons.csv", low_memory=False)

    old_claims = pd.read_csv("/Users/ludovicocesaro/Downloads/enlarged/claims.csv", low_memory=False)
    new_claims = pd.read_csv("/Users/ludovicocesaro/Downloads/anonymized/2/claims.csv", low_memory=False)

    # _2 Containing ALL (new and old concatenated)
    po_frames = [old_policies, new_policies]
    policies_2 = pd.concat(po_frames)

    pe_frames = [old_persons, new_persons]
    persons_2 = pd.concat(pe_frames)

    claims_frames = [old_claims, new_claims]
    claims_2 = pd.concat(claims_frames)

    # Removing unnecessary columns
    policies_2.drop('Unnamed: 0', axis=1, inplace=True)
    persons_2.drop('Unnamed: 0', axis=1, inplace=True)
    claims_2.drop('Unnamed: 0', axis=1, inplace=True)

    # Taking random ids from persons to make them match
    random_persons = new_persons.sample(n = 281533)
    random_persons.reset_index(inplace=True)
    random_persons['ID'].head(10)

    # Taking random ids from policies to make them match
    random_policies = new_policies.sample(n = 281533)
    random_policies.reset_index(inplace=True)
    random_policies['ID'].head(10)

    # Add random PERSONS from persons new for matching
    # Add random POLICY ID from policies new for matching 
    # with pd.option_context('mode.chained_assignment',None):
    replace_pp = claims_2[281533:] # Edit the number putting the last item of the previous run
    replace_pp.reset_index(inplace=True)
    replace_pp['PERSON_ID'] = random_persons['ID'].copy()
    replace_pp['POLICY_ID'] = random_policies['ID'].copy()
    replace_pp.drop(['index'], axis = 1, inplace=True)
    new_matched_claims = replace_pp
    new_matched_claims # has random person_id

    # claims enlarged dataframe with PERSON_ID and POLICY_ID primary key coming from persons and policies for matching
    frames = [old_claims, new_matched_claims]
    claims2_new = pd.concat(frames)
    claims2_new.reset_index(inplace=True)
    claims2_new.drop(['index','Unnamed: 0'], axis = 1, inplace=True)
    claims2_new
    #claims2_new.to_csv("/Users/ludovicocesaro/Downloads/enlarged/claims.csv")

generate_anonymized_data()
#aggregate()
