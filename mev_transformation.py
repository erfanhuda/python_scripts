import itertools
import pandas as pd
import glob
import os

<<<<<<< HEAD:mev_transformation.py
SOURCE_PATH = "X:/dev/app/python_scripts/file/input/mev/"
OUTPUT_PATH = "X:/dev/app/python_scripts/file/output/mev/"
OUTPUT_FILE = "combine_variable2.xlsx"
=======
SOURCE_PATH = "C:/Users/muhammad.huda/OneDrive - Seagroup/01. WORK/01. ECL/Remodelling FY 23/Q3/raw data/Up to Sep 2023/MEV/Wrapped/csv/worst"
OUTPUT_PATH = "C:/Users/muhammad.huda/OneDrive - Seagroup/01. WORK/01. ECL/Remodelling FY 23/Q3/raw data/Up to Sep 2023/MEV/Wrapped/csv/output"
OUTPUT_FILE = "py_MEV_WORST_transformation_Q3.xlsx"
>>>>>>> b9751c26959621c5541561d01e46b2f182c55f6c:variable_transformation.py

variable_path = glob.glob(os.path.join(SOURCE_PATH, "*.csv"))
list_df = []

# First CSV
for item in variable_path:
    df = pd.read_csv(item, parse_dates=['Date'], index_col=["Date"])
    list_df.append(df)

if len(list_df) > 1:
    frame = pd.concat(list_df, axis=1)
else:
    frame = list_df[0]

dt = pd.date_range(frame.index[0], frame.index[-1],
                   freq="M").to_frame(name="Date")
frame = pd.merge(dt, frame, left_index=True, right_index=True)
frame = frame.drop(columns="Date").interpolate(method="linear")

period = {
    "1Q": 3,
    "2Q": 6,
    "3Q": 9,
    "4Q": 12,
    "1H": 6,
    "2H": 12,
    "3H": 18,
    "4H": 24,
    "1Y": 12,
    "2Y": 24,
    "3Y": 36,
    "4Y": 48,
    "1M": 1,
    "2M": 2,
    "3M": 3,
    "4M": 4,
    "5M": 5,
    "6M": 6,
    "7M": 7,
    "8M": 8,
    "9M": 9,
    "10M": 10,
    "11M": 11,
    "12M": 12
}

configuration = {
    "lag": {
        "1M": "mom",
        "3M": "qoq",
        "6M": "hoh",
        "9M": "sos",
        "12M": "yoy",
    },
    "delta": {
        "1M": True,
        "3M": True,
        "6M": True,
        "9M": True,
        "12M": True,
    },
    "growth": {
        "1M": True,
        "3M": True,
        "6M": True,
        "9M": True,
        "12M": True
    }
}

list_config = ["_".join([item, litem])
               for item in configuration for litem in configuration[item] if configuration[item][litem]]

adv_config = itertools.combinations(list_config, r=2)
adv_config = ["_".join([item[1], item[0].split("_")[1]])
              for item in adv_config if "lag" in item[0] if "lag" not in item[1]]

list_config = list_config + adv_config
adv_headers = ["_".join([item[0], item[1]]) for item in itertools.product(
    frame.columns.to_list(), list_config)]
adv_headers = pd.DataFrame(dt, columns=adv_headers)

frame = pd.concat([frame, adv_headers], axis=1)


def add_growth(x, y):
    return (x / y) - 1


def add_delta(x, y):
    return x - y


frame_config = []
for col in frame:
    if "lag" in col:
        c = col.split("_")[2]
        m = c.split("M")[0]
        frame_config.append(tuple([col, m]))

        frame[col] = frame[col.split("_")[0]].shift(int(m))

    elif "delta" in col:
        c = col.split("_")
        frame_config.append(tuple([col, c[2]]))

        if c[2] != c[-1]:
            frame[col] = frame[f"{c[0]}_lag_{c[2]}"] - \
                frame[f"{c[0]}_lag_{c[2]}"].shift(period.get(c[-1]))

            frame[col].rename(
                "_".join([c[0], c[1], "lag", configuration['lag'][c[2]], c[3]]), inplace=True)

        else:
            # print(f"{c[0]}")
            frame[col] = frame[f"{c[0]}"] - \
                frame[f"{c[0]}"].shift(period.get(c[-1]))

    elif "growth" in col:
        c = col.split("_")
        frame_config.append(tuple([col, c[2]]))

        if c[2] != c[-1]:
            # print(f"{c[0]}_lag_{c[-1]}")
            frame[col] = (frame[f"{c[0]}_lag_{c[-1]}"] /
                          frame[f"{c[0]}_lag_{c[-1]}"].shift(period.get(c[2]))) - 1

            frame[col].rename(
                "_".join([c[0], c[1], "lag", configuration['lag'][c[2]], c[3]]), inplace=True)
        else:
            frame[col] = (
                frame[f"{c[0]}"] / frame[f"{c[0]}"].shift(period.get(c[-1]))) - 1
    else:
        frame_config.append(tuple([col, 0]))

frame.interpolate(method="linear").to_excel(
    os.path.join(OUTPUT_PATH, OUTPUT_FILE))

training_date = "2023-09-30"
testing_date = "2023-10-31"
