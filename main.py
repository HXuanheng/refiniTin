import refinitiv.dataplatform as rdp
import os

rdp.open_platform_session(
    "2b671088688c46f48d63b3813f000068175490d7",
    rdp.GrantPassword(
        "xuanheng.huang@studbocconi.it",
        "Dadada96!"
    )
)

# rdp.open_desktop_session('2b671088688c46f48d63b3813f000068175490d7')

# create the directory if it doesn't exist
directory = "filings"
if not os.path.exists(directory):
    os.makedirs(directory)

def main():
    # query = """
    #         SELECT *
    #         FROM EDGAR
    #         AND EDGARFormType = '8-K'
    #         AND EDGAREffectiveDate >= '2022-01-01'
    #         AND EDGAREffectiveDate <= '2022-02-01'
    #         """
    query = "SELECT * FROM NewsHeadline WHERE MATCH('merger OR acquisition')"
    response = rdp.search(query)
    print(response)
    for i, result in enumerate(response):
        # content = rdp.get(result['Link']).decode('utf-8')
        accession_number = content.split('ACCESSION NUMBER: ')[1].split('\n')[0]
        filename = f"{directory}/{accession_number}.txt"
        with open(filename, 'w') as f:
            f.write(content)
        print(f"Saved filing {i+1} to {filename}")
    # response = rdp.get_data(universe=["TRI.N", "IBM.N"], fields=["TR.Revenue", "TR.GrossProfit"])
    # print(response)


if __name__ == "__main__":
    main()