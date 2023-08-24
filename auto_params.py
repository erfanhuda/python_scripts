import argparse
from csv import DictWriter
import csv
from itertools import product
from calendar import monthrange
from datetime import date, timedelta
import pandas as pd

# INPUT VARIABLES
DATE = [ x for x in pd.date_range(start="2021-07-31", end="2023-06-30", freq="M").strftime("%Y/%m/%d")]
BUCKET = [x for x in range(1, 6)]
PRODUCT = ["Digital_SCL"] 
TENOR = [3,6,12]
SW = ["BASE", "BEST", "WORST"]
base_cpd_eml = [ 0.0039,0.0486,0.0600,0.1098,1.0000 ]
base_cpd_kpl = [ 0.0000,0.0018,0.0025,0.0067,1.0000 ]

parser = argparse.ArgumentParser()

def set_date(**kwargs):
    return [ x for x in pd.date_range(start=kwargs.get("start"), end=kwargs.get("end"), freq="M").strftime("%Y/%m/%d")]

def set_product(**kwargs):
    product = []
    try:
        if isinstance(kwargs.get("product"), list):
            # do something with list / multiple
            product = product + kwargs.get("product")
        elif isinstance(kwargs.get("product"),str):
            # do something with string / singular
            product.append(kwargs.get("product"))
        else:
            raise ValueError("Invalid tokens")
        
    except Exception as e:
        print(e)

    finally:
        return product

# FUNCTIONS
def populate_scenarios():
    with open('py_load_pd_eml.csv', 'w', newline='') as file:
        fieldnames = ["scenario", "pd_segment", "tenor", "period", "ecl_bucket_client", "pd_base", "pd_best", "pd_worst"]
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        
        writer.writeheader()
        for item in product(BUCKET, PRODUCT, SW, range(2, 61)):
            bucket = item[0]
            prd_code = item[1]
            scenario = item[2]
            tenor = item[3]
            period = 12

            writer.writerow({"scenario": scenario, "pd_segment": prd_code, "tenor": tenor, "period": period,"ecl_bucket_client": bucket})

def auto_populate(**kwargs):
    try:
        products = kwargs.get("product")
        tenors = kwargs.get("tenor")

    except ValueError as e:
        print(e)
    finally:
        with open("./result/csv_templates.csv", "w", newline='') as file:
            fieldnames = ["tenor", "pd_segment",  "bucket_from", "bucket_to", "pt_date"]
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            print(writer.writeheader())
            # writer.writeheader()
            
            for item in product(tenors, products, BUCKET, BUCKET, DATE):
                tenor = item[0]
                products = item[1]
                bucket_from = item[2]
                bucket_to = item[3]
                pt_date = item[4]



                print({"tenor": tenor, "pd_segment": products, "bucket_from": bucket_from, "bucket_to": bucket_to, "pt_date": pt_date})
                # writer.writerow({"tenor": tenor, "pd_segment": products, "bucket_from": bucket_from, "bucket_to": bucket_to, "pt_date": pt_date})


parser.add_argument("--run", type=str)
args = parser.parse_args()

# match args:
#     case "--exploit": auto_populate()
#     case "--products": set_product()



new_date = set_date(start="2023-01-31", end="2023-05-31")
products = set_product(product=["Digital_BCL", "Digital_SPL"])
# populasi = auto_populate(product="Digital_BCL", tenor=[1,2,3])