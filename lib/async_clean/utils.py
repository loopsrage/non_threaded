import asyncio
import logging

import pandas as pd
import seaborn as sns
from matplotlib.backends.backend_agg import FigureCanvasAgg
from matplotlib.figure import Figure
from pandas.core.interchange.dataframe_protocol import DataFrame

from lib.fsspecclean import FSpecFS

def feature_mask(data, cvi=None, skew=None, riqr=None):
    if cvi is None:
        cvi = .3

    if skew is None:
        skew = 1.

    if riqr is None:
        riqr = .5

    numeric_df = data.select_dtypes(include='number')
    cv = numeric_df.std() / numeric_df.mean().abs()
    skewness = numeric_df.skew()
    iqr = numeric_df.quantile(0.75) - numeric_df.quantile(0.25)
    relative_iqr = iqr / numeric_df.median().abs()
    wm = (cv > cvi) & (skewness.abs() > skew) & (relative_iqr > riqr)
    return wm[wm].index.tolist()

def convert_numeric(data: pd.DataFrame, target_columns: list = None) -> pd.DataFrame:
    df = data.copy()
    for col in data.columns:
        numeric_series = pd.to_numeric(df[col], errors='coerce')
        if numeric_series.notna().any():
            df[col] = numeric_series.fillna(numeric_series.mean())

    if target_columns:
        df = df.reindex(columns=target_columns, fill_value=0)
    return df

def auto_extract_dates(data):
    date_columns = []
    df = data.copy()
    for col in data.select_dtypes(include="object").columns:
        selcol = df[col]
        hms = pd.to_timedelta(selcol, errors='coerce')
        if hms.notna().any():
            df[f'hour'] = hms.dt.components['hours']
            df[f'minute'] = hms.dt.components['minutes']
            date_columns.append(col)

        dt_series = pd.to_datetime(selcol, errors='coerce')
        if dt_series.notna().any():
            df[f'year'] = dt_series.dt.year
            df[f'month'] = dt_series.dt.month
            df[f'day'] = dt_series.dt.day
            df[f'day_of_week'] = dt_series.dt.dayofweek
            date_columns.append(col)

    df = df.drop(columns=date_columns, errors='ignore')
    return df

def encode_png():
    pass
    # img_buffer = io.BytesIO()
    # fig.savefig(img_buffer, format='png')
    # img_buffer.seek(0)
    # img_str = base64.b64encode(img_buffer.getvalue()).decode("utf-8")

async def clean_pipeline(input_df: DataFrame, storage: FSpecFS, request_id):
    def single_feature_pair_plot(data, feature, target):
        logging.info(f"received single feature pair plot request {request_id}")

        fig = Figure(figsize=(24, 6))
        canvas = FigureCanvasAgg(fig)
        ax = fig.add_subplot(111)
        # Create the figure object directly (Thread-safe)
        try:
            png_name = f"{feature}_vs_{target}.png"
            sns.scatterplot(data=data, x=feature, y=target, alpha=.3, ax=ax)
            sns.regplot(data=data, x=feature, y=target, scatter=False, color='red', ax=ax)
            storage.save_png_file(request_id, png_name, fig, use_pipe=True)
        except Exception as e:
            logging.error(f"Failed to generate plot for {request_id}: {e}")
            raise
        finally:
            # In the OO API, there is no plt.close().
            # Just clear the figure to ensure internal refs are dropped immediately.
            fig.clear()
            ax.cla()
            del fig, ax, canvas

    async def separate_features_targets(df: pd.DataFrame):
        logging.info(f"received feature target request {request_id}")

        mask = await asyncio.to_thread(feature_mask, df)
        targets = df[mask]
        features = await asyncio.to_thread(df.drop, columns=mask)

        tasks = []
        async with asyncio.TaskGroup() as tg:
            for ti in targets:
                for fi in features:
                    tasks.append(tg.create_task(asyncio.to_thread(single_feature_pair_plot, df, fi, ti)))

        return targets, features, [[t.result() for t in tasks]]

    async def clean_df(df: pd.DataFrame):
        logging.info(f"received clean request {request_id}")
        df = await asyncio.to_thread(convert_numeric, df)
        df = await asyncio.to_thread(auto_extract_dates, df)
        storage.save_clean_file(request_id=request_id, data=df, use_pipe=True)
        return await separate_features_targets(df)

    return await clean_df(input_df)