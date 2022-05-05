import pandas as pd
pd.set_option("display.max_columns", None)
byte = 1000000
def generate_data():
    """
    Function that generates more anonymized data starting from a base ridm file already anonymized adding characters 'ABC' then compound it
    for defined times then call fix_size()
    """
    times = int(
        input(
            "Please enter how many times you want to enlarge exponentially the initial datasets\n"
        )
    )
    print(f"The enlargment will be compounded by {times} times")

    claims = pd.read_csv("/Users/ludovicocesaro/Downloads/test/0/claims.csv")
    persons = pd.read_csv("/Users/ludovicocesaro/Downloads/test/0/persons.csv")
    policies = pd.read_csv("/Users/ludovicocesaro/Downloads/test/0/policies.csv")

    print("Tables before enlargement")
    print("Size claims:", claims.memory_usage().sum() / byte, "Mb")
    print("Size persons:", persons.memory_usage().sum() / byte, "Mb")
    print("Size policies:", policies.memory_usage().sum() / byte, "Mb")
    for x in range(times):
        index = len(claims.index)
        # Creating new claims, persons , policies + dummy primary keys creation
        new_claims = claims.copy()
        new_claims["ID"] = new_claims["ID"] + "ABC"
        new_persons = persons.copy()
        new_persons["ID"] = new_persons["ID"] + "ABC"
        new_policies = policies.copy()
        new_policies["ID"] = new_policies["ID"] + "ABC"
        # Taking random ids from persons and policies to make them match with claims foreign keys
        random_persons = new_persons.sample(n=index * pow(2, x), replace=True) # index with pow for compounding
        random_persons.reset_index(inplace=True)
        random_policies = new_policies.sample(n=index * pow(2, x), replace=True)
        random_policies.reset_index(inplace=True)
        # Replacing keys to new generated claims
        new_claims["PERSON_ID"] = random_persons["ID"]
        new_claims["POLICY_ID"] = random_policies["ID"]
        claims_frames = [claims, new_claims]  # New claims and old claims merged
        new_matched_claims = pd.concat(claims_frames)
        new_matched_claims.reset_index(inplace=True)
        new_matched_claims.drop(["index"], axis=1, inplace=True)
        claims = new_matched_claims
        print(new_matched_claims)
        persons_frames = [persons, new_persons]
        concat_persons = pd.concat(persons_frames)
        policies_frames = [policies, new_policies]
        concat_policies = pd.concat(policies_frames)
    fix_size(new_matched_claims, concat_persons, concat_policies)

def fix_size(claims, persons, policies):
    """
    Function called after generate_data(), asks if the size is okay otherwise reduce the size by percentage, remove unnecessary columns and save as csv
    """
    claims_columns = [
        "ID",
        "PERSON_ID",
        "PROVIDER_ID",
        "POLICY_ID",
        "LOCAL_CLAIM_ID",
        "CLAIM_COUNTRY",
        "LOCAL_COVERAGE",
        "NETWORK_TREATMENT",
        "NETWORK_TYPE",
        "TREATMENT_START_DATE",
        "TREATMENT_END_DATE",
        "BILLING_DATE",
        "SUBMISSION_DATE",
        "PRE_APPROVAL_DATE",
        "APPROVAL_DATE",
        "SETTLEMENT_DATE",
        "CLAIM_AMOUNT",
        "AMOUNT_PAID_BY_CAPTIVE",
        "REIMBURSED_AMOUNT",
        "REIMBURSABLE_AMOUNT",
        "CURRENCY_CLAIM_AMOUNT",
        "CURRENCY_AMOUNT_PAID_BY_CAPTIVE",
        "CURRENCY_REIMBURSED_AMOUNT",
        "CURRENCY_REIMBURSABLE_AMOUNT",
        "REIMBURSEMENT_TYPE",
        "CLAIM_STATUS",
        "CLAIM_REJECTION_REASON",
        "MEDICAL_AREA",
    ]
    persons_columns = [
        "ID",
        "DUMMY_PERSON_FLAG",
        "DATE_OF_BIRTH",
        "MEMBER_TYPE",
        "GENDER",
        "OCCUPATION",
        "LOCATION",
        "LOCATION_TYPE",
        "REGION",
    ]
    policies_columns = [
        "ID",
        "BUSINESS_RELATION_ID",
        "CERTIFICATE_ID",
        "CONTRACT_ID",
        "PRODUCT_ID",
        "LOCAL_POLICY_ID",
        "NO_OF_RISKS",
        "POLICY_START_DATE",
        "POLICY_END_DATE",
        "NEW_BUSINESS",
    ]

    tables = [
        {"tableName": "claims", "dataframe": claims, "columns": claims_columns},
        {"tableName": "persons", "dataframe": persons, "columns": persons_columns},
        {"tableName": "policies", "dataframe": policies, "columns": policies_columns},
    ]
    print("Tables after enlargement")
    print("Size claims:", claims.memory_usage().sum() / byte, "Mb")
    print("Size persons:", persons.memory_usage().sum() / byte, "Mb")
    print("Size policies:", policies.memory_usage().sum() / byte, "Mb")
    print('Saving...')
    answer = input("Do you accept the current sizes? y/n: ")
    if answer == "n":
        decrease = int(
            input("How much do you want to decrease the size in percentage? ")
        )
        for data in tables:
            total = len(data["dataframe"].index)
            toremove = round(total * decrease / 100)
            last = total - toremove
            #final_decreased = data["dataframe"][:last]
            #print(final_decreased, 'Increased')
            print(
                data["tableName"],
                "reduced. Current size",
                data["dataframe"][:last].memory_usage().sum() / byte,
                "Mb",
            )

    for data in tables:
        data['dataframe'].drop(
        columns=[col for col in data['dataframe'] if col not in data['columns']],
        inplace=True,
    )
        data["dataframe"].to_csv(("/Users/ludovicocesaro/Downloads/test/0/{}.csv").format(data["tableName"]))
        print("Table", data["tableName"],"correctly saved")
    raise SystemExit
        
generate_data()
