import pandas as pd

pd.set_option("display.max_columns", None)


def generate_anonymized_data():
    """
        Function that generates more anonymized data starting from a base ridm file already anonymized adding characters 'ABC' then combine it
    for times defined
    """
    times = int(
        input(
            "Please enter how many times you want to enlarge the datasets: (exponential)\n"
        )
    )
    print(f"The enlargment will be compounded by {times} times")

    policies = pd.read_csv("/Users/ludovicocesaro/Downloads/test/0/policies.csv")
    claims = pd.read_csv("/Users/ludovicocesaro/Downloads/test/0/claims.csv")
    persons = pd.read_csv("/Users/ludovicocesaro/Downloads/test/0/persons.csv")

    for x in range(times):

        index = 281533
        # Creating new claims, persons , policies primary keys
        new_claims = claims.copy()
        new_claims["ID"] = new_claims["ID"] + "ABC"
        new_persons = persons.copy()
        new_persons["ID"] = new_persons["ID"] + "ABC"
        new_policies = policies.copy()
        new_policies["ID"] = new_policies["ID"] + "ABC"

        # Taking random ids from persons and policies to make them match with claims foreign keys
        random_persons = new_persons.sample(n=index * pow(2, x), replace=True)
        random_persons.reset_index(inplace=True)

        random_policies = new_policies.sample(n=index * pow(2, x), replace=True)
        random_policies.reset_index(inplace=True)

        # Replacing correct keys to new generated claims
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

    new_matched_claims.drop(
        columns=[col for col in new_matched_claims if col not in claims_columns],
        inplace=True,
    )
    concat_persons.drop(
        columns=[col for col in concat_persons if col not in persons_columns],
        inplace=True,
    )
    concat_policies.drop(
        columns=[col for col in concat_policies if col not in policies_columns],
        inplace=True,
    )

    fix_size(new_matched_claims, concat_persons, concat_policies)  # correct


# TODO Finish fix size
def fix_size(claims, persons, policies):

    tables = [claims, persons, policies]
    byte = 1000000

    print("Tables after enlargement")
    print("Size claims:", claims.memory_usage().sum() / byte, "Mb")
    print("Size persons:", persons.memory_usage().sum() / byte, "Mb")
    print("Size policies:", policies.memory_usage().sum() / byte, "Mb")

    answer = input("Do you accept the current sizes? y/n: ")

    if answer == "y":
        raise SystemExit

    else:
        decrease = int(
            input("How much do you want to decrease the size in percentage?")
        )
        for data in tables:
            total = len(data.index)
            toremove = round(total * decrease / 100)
            last = total - toremove
            final_decreased = data[:last]
            # FORMAT DOES NOT WORK, MUST HAVE TABLE NAME NOT DATAFRAME
            name = data.tablename
            final_decreased.to_csv(
                ("/Users/ludovicocesaro/Downloads/test/0/{}.csv").format(name)
            )
            print(
                name,
                "reduced and saved. Current size",
                name.memory_usage().sum() / byte,
                "Mb",
            )


generate_anonymized_data()
