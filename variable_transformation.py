import itertools
import pandas as pd
import glob
import os

variable_path = glob.glob(os.path.join(r"X:/dev/app/python_scripts/file/input/mev/", "*.csv"))
list_df = []

# First CSV 
for item in variable_path:
    df = pd.read_csv(item, parse_dates=['Date'],index_col=["Date"])
    list_df.append(df)

if len(list_df) > 1:
    frame = pd.concat(list_df, axis=1)
else:
    frame = list_df[0]

dt = pd.date_range(frame.index[0], frame.index[-1], freq="M").to_frame(name="Date")
frame = pd.merge(dt, frame, left_index=True, right_index=True)
frame = frame.drop(columns="Date").interpolate(method="linear")

period = {
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
    "12M": 12,
    "1Q": 3,
    "2Q": 6,
    "3Q": 9,
    "4Q": 12,
    "1Y": 12,
    "2Y": 24,
    "3Y": 36,
    "4Y": 48
}

configuration = {
    "lag" : {
        "1M":"mom",
        "3M": "qoq",
        "6M": "hoh",
        "12M": "yoy",
    },
    "delta": {
        "1Q": True,
        "2Q": True,
        "3Q": True,
        "4Q": True,
    },
    "growth": {
        "1Q": True,
        "2Q": True,
        "3Q": True,
        "4Q": True
    }
}

list_config = ["_".join([item, litem]) for item in configuration for litem in configuration[item] if configuration[item][litem]]

adv_config = itertools.combinations(list_config, r=2)
adv_config = ["_".join([item[1], item[0].split("_")[1]]) for item in adv_config if "lag" in item[0] if "lag" not in item[1]]

list_config = list_config + adv_config
adv_headers = ["_".join([item[0], item[1]]) for item in itertools.product(frame.columns.to_list(), list_config) ]
adv_headers = pd.DataFrame(dt, columns=adv_headers)

frame = pd.concat([frame, adv_headers], axis=1)

def add_growth(x, y):
    return (x / y) -1

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
            # print(f"{c[0]}_lag_{c[-1]}")
            frame[col] = frame[f"{c[0]}_lag_{c[-1]}"] - frame[f"{c[0]}_lag_{c[-1]}"].shift(period.get(c[2]))
        else:
            # print(f"{c[0]}")
            frame[col] = frame[f"{c[0]}"] - frame[f"{c[0]}"].shift(period.get(c[-1]))

    elif "growth" in col:
        c = col.split("_")
        frame_config.append(tuple([col, c[2]]))
        
        if c[2] != c[-1]:
            # print(f"{c[0]}_lag_{c[-1]}")
            frame[col] = (frame[f"{c[0]}_lag_{c[-1]}"] / frame[f"{c[0]}_lag_{c[-1]}"].shift(period.get(c[2]))) - 1
        else:
            # print(f"{c[0]}")
            frame[col] = (frame[f"{c[0]}"] / frame[f"{c[0]}"].shift(period.get(c[-1]))) - 1

    else:
        frame_config.append(tuple([col, 0]))

frame.to_excel("X:/dev/app/python_scripts/file/output/mev/combine_variable2.xlsx")