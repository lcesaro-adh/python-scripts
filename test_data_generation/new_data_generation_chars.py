import pandas as pd
pd.set_option("display.max_columns", None)
pd.options.mode.chained_assignment = None

byte = 1000000
def generate_data():
    """
    Function that generates more anonymized data starting from a base ridm file already anonymized adding characters 'ABC' then compound it
    for defined times then call fix_size()
    """
    print('reading tables...') 
    claims = pd.read_csv("test_data_generation/input/claims.csv")
    persons = pd.read_csv("test_data_generation/input/persons.csv")
    policies = pd.read_csv("test_data_generation/input/policies.csv")

    print("Tables before enlargement")
    print("Size claims:", claims.memory_usage().sum() / byte, "Mb")
    print("Size persons:", persons.memory_usage().sum() / byte, "Mb")
    print("Size policies:", policies.memory_usage().sum() / byte, "Mb")

    times = int(
        input(
            "Please enter how many times you want to enlarge exponentially the initial datasets\n"
        )
    )
    print(f"The enlargment will be compounded by {times} times")

    for x in range(times):
        index = len(claims.index) #Change (index of the table that contains both)
        # Creating new claims, persons , policies + dummy primary keys creation
        new_claims = claims.copy()
        new_persons = persons.copy()
        new_persons["ID"] = new_persons["ID"] + "ABC"
        new_policies = policies.copy()
        new_policies["ID"] = new_policies["ID"] + "ABC"
        new_claims["PERSON_ID"] = new_persons["ID"]
        new_claims["POLICY_ID"] = new_policies["ID"]
        new_matched_claims = pd.concat([claims, new_claims]) # New claims and old claims merged
        new_matched_claims.reset_index(inplace=True)
        new_matched_claims.drop(["index"], axis=1, inplace=True)
        claims = new_matched_claims
        print(new_matched_claims)
        concat_persons = pd.concat([persons, new_persons])
        concat_policies = pd.concat([policies, new_policies])
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
#------ 
    print("Tables after enlargement")
    for data in tables:
        print("Size",data["tableName"],data["dataframe"].memory_usage().sum() / byte, "Mb")

    answer = input("Do you accept the current sizes? y/n: ")
    if answer == "n":
        decrease = int(
            input("How much do you want to decrease the size in percentage? ")
        )
        for data in tables: # decrease size
            total = len(data["dataframe"].index)
            toremove = round(total * decrease / 100)
            last = total - toremove
            data_decreased = data["dataframe"][:last]
            print(
                "Saving...", data["tableName"],
                "reduced. Current size",
                data_decreased.memory_usage().sum() / byte,
                "Mb",
            )
            data_decreased.drop(
            columns=[col for col in data['dataframe'] if col not in data['columns']],
            inplace=True,
            )
            data_decreased.to_csv(("test_data_generation/output/{}.csv").format(data["tableName"]))
    else:
        for data in tables:
            data['dataframe'].drop(
            columns=[col for col in data['dataframe'] if col not in data['columns']],
            inplace=True,
            )
            data["dataframe"].to_csv(("test_data_generation/output/{}.csv").format(data["tableName"]))
            print("Table", data["tableName"],"correctly saved")
        
generate_data()