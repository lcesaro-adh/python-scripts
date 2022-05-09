import pandas as pd
import subprocess
import os
import sys

pd.set_option("display.max_columns", None)
d = dict(os.environ)
# # SETUP Virtual Environment
# py_env = subprocess.check_call([sys.executable,"-m","pyenv", "activate", "dp"])
# py_path = subprocess.check_call([sys.executable,"-m","export", "PYTHONPATH=$PWD:$PYTHONPATH"])
# py_path2 = subprocess.check_call([sys.executable,"-m","echo", "$PYTHONPATH"])
# #env_req = subprocess.run(["pip", "install", "-r", "/Users/ludovicocesaro/Desktop/Files/Reply/Allianz/Projects/datahub-pipelines/requirements.txt"], shell=True, capture_output=True)
# env_req = subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "/Users/ludovicocesaro/Desktop/Files/Reply/Allianz/Projects/datahub-pipelines/requirements.txt"])
# print(py_env, py_path,py_path2, env_req,'status setup')

def generate_anonymized_data():
    """
	Function that generates more anonymized data starting from a base ridm file already anonymized then combine it.

    The function asks for how many times the base ridm file want to be enlarged.
    """ 
    times = int(input("Please enter how many times you want to enlarge the datasets:\n"))
    print(f"You choosed to enlarge by {times} times")

    for x in range(times):
        print('Anonymization run', x+1)
        # RUN ANONYMIZATION SCRIPT
        # #TODO: FIX python_env & env_var / make work command
        # next = x+1
        # anonymize = subprocess.run(
        #     [
        #         f"python",
        #         "/Users/ludovicocesaro/Desktop/Files/Reply/Allianz/Projects/datahub-pipelines/tasks/common/anonymization/secure_anonymize.py",
        #         "/Users/ludovicocesaro/Downloads/test/{}".format(x),
        #         "/Users/ludovicocesaro/Downloads/test/{}".format(next)
        #     ])
        # # command for anonymization
        # # TODO: path must be generalized for working in all environments

    combine(times)

def combine(times):
    for x in range(times):
        print(x, 'time')
        # Extracting the old policies and new
        old_policies = pd.read_csv("/Users/ludovicocesaro/Downloads/test/{}/policies.csv".format(x))
        new_policies = pd.read_csv("/Users/ludovicocesaro/Downloads/test/{}/policies.csv".format(x+1))

        old_persons = pd.read_csv("/Users/ludovicocesaro/Downloads/test/{}/persons.csv".format(x))
        new_persons = pd.read_csv("/Users/ludovicocesaro/Downloads/test/{}/persons.csv".format(x+1))

        old_claims = pd.read_csv("/Users/ludovicocesaro/Downloads/test/{}/claims.csv".format(x))
        new_claims = pd.read_csv("/Users/ludovicocesaro/Downloads/test/{}/claims.csv".format(x+1))

        index = 281533 # Base index

        # _2 Containing ALL (new and old concatenated)
        po_frames = [old_policies, new_policies]
        policies_2 = pd.concat(po_frames)

        pe_frames = [old_persons, new_persons]
        persons_2 = pd.concat(pe_frames)

        claims_frames = [old_claims, new_claims]
        claims_2 = pd.concat(claims_frames)

        # Removing unnecessary columns
        # policies_2.drop('Unnamed: 0', axis=1, inplace=True)
        # persons_2.drop('Unnamed: 0', axis=1, inplace=True)
        # claims_2.drop('Unnamed: 0', axis=1, inplace=True)

        # Taking random ids from persons to make them match
        random_persons = new_persons.sample(n = round(index/(x+1))) #TODO: Check
        random_persons.reset_index(inplace=True)
        random_persons['ID'].head(10)

        # Taking random ids from policies to make them match
        random_policies = new_policies.sample(n = round(index/(x+1))) #TODO: Check
        random_policies.reset_index(inplace=True)
        random_policies['ID'].head(10)


        # Add random PERSONS from persons new for matching
        # Add random POLICY ID from policies new for matching 
        with pd.option_context('mode.chained_assignment',None):
            index = index*(x+1) #TODO Check dynamic index if it's working always /  check number if its really progressive after runs
            print(index, "INDEX")
            replace_pp = claims_2[index:]
            replace_pp.reset_index(inplace=True)
            replace_pp['PERSON_ID'] = random_persons['ID'].copy()
            replace_pp['POLICY_ID'] = random_policies['ID'].copy()
            #replace_pp.drop(['index'], axis = 1, inplace=True)
            new_matched_claims = replace_pp
            new_matched_claims # has random person_id

        # claims enlarged dataframe with PERSON_ID and POLICY_ID primary key coming from persons and policies for matching
        frames = [old_claims, new_matched_claims]
        claims2_new = pd.concat(frames)
        claims2_new.reset_index(inplace=True)
        claims2_new.drop(["level_0","index"], axis = 1, inplace=True)
        print("Claims dataset enlarged and matched", claims2_new)
        claims2_new.to_csv(("/Users/ludovicocesaro/Downloads/test/{}/claims.csv").format(x+1))
        persons_2.to_csv(("/Users/ludovicocesaro/Downloads/test/{}/persons.csv").format(x+1))
        policies_2.to_csv(("/Users/ludovicocesaro/Downloads/test/{}/policies.csv").format(x+1))
        print("Combination of the anonymized dataset completed", x+1, "run(s) of ", times)
    print('ok')
generate_anonymized_data()