import pandas as pd
pd.set_option("display.max_columns", None)

def generate_anonymized_data():
    """
    Function that generates more anonymized data starting from a base ridm file already anonymized adding characters 'ABC' then combine it
    for times defined
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
    print("Size claims:", claims.memory_usage().sum() / 1000000, "Mb")
    print("Size persons:", persons.memory_usage().sum() / 1000000, "Mb")
    print("Size policies:", policies.memory_usage().sum() / 1000000, "Mb")

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

    tables = [
        {"tableName": "claims", "dataframe": claims},
        {"tableName": "persons", "dataframe": persons},
        {"tableName": "policies", "dataframe": policies},
    ]
    byte = 1000000

    print("Tables after enlargement")
    print("Size claims:", claims.memory_usage().sum() / byte, "Mb")
    print("Size persons:", persons.memory_usage().sum() / byte, "Mb")
    print("Size policies:", policies.memory_usage().sum() / byte, "Mb")

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
        data["dataframe"].to_csv(("/Users/ludovicocesaro/Downloads/test/0/{}.csv").format(data["tableName"]))
        print("Table,", data["tableName"],"correctly saved")
        raise SystemExit
        

generate_anonymized_data()
