import os
from datetime import datetime

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
import streamlit as st


st.set_page_config(
    page_title="Real-Time Fraud Detection System",
    page_icon="RF",
    layout="wide",
    initial_sidebar_state="expanded",
)


@st.cache_resource
def get_postgres_connection():
    try:
        return psycopg2.connect(
            host=os.environ.get("POSTGRES_HOST", "postgres"),
            database=os.environ.get("POSTGRES_DB", "streaming"),
            user=os.environ.get("POSTGRES_USER", "postgres"),
            password=os.environ.get("POSTGRES_PASSWORD", "password"),
            port=os.environ.get("POSTGRES_PORT", "5432"),
        )
    except Exception as exc:
        st.error(f"Cannot connect to PostgreSQL: {exc}")
        return None


def add_status_columns(df):
    if df.empty:
        return df

    enriched = df.copy()
    enriched["fraud_signal"] = (enriched["prediction"].fillna(0) >= 1).astype(int)
    enriched["fraud_probability"] = (enriched["prob_1"].fillna(0) * 100).round(2)
    enriched["status"] = enriched["fraud_signal"].map({1: "ALERT", 0: "CLEAR"})
    return enriched


@st.cache_data(ttl=60)
def get_transactions(limit=1000, fraud_only=False):
    conn = get_postgres_connection()
    if not conn:
        return pd.DataFrame()

    query = """
        SELECT trans_num, trans_date_trans_time, cc_num, amt, merchant, category,
               is_fraud, prediction, prob_0, prob_1
        FROM fraud_predictions
    """
    if fraud_only:
        query += " WHERE prediction >= 1"
    query += " ORDER BY trans_date_trans_time DESC LIMIT %s"

    try:
        df = pd.read_sql_query(query, conn, params=[limit])
        return add_status_columns(df)
    except Exception as exc:
        st.error(f"Error fetching transactions: {exc}")
        return pd.DataFrame()


@st.cache_data(ttl=30)
def get_statistics():
    conn = get_postgres_connection()
    if not conn:
        return {
            "total_transactions": 0,
            "predicted_fraud_transactions": 0,
            "predicted_fraud_rate": 0.0,
            "avg_transaction_amount": 0.0,
            "avg_fraud_probability": 0.0,
        }

    query = """
        SELECT
            COUNT(*) AS total_transactions,
            SUM(CASE WHEN prediction >= 1 THEN 1 ELSE 0 END) AS predicted_fraud_transactions,
            AVG(amt) AS avg_transaction_amount,
            AVG(prob_1) AS avg_fraud_probability
        FROM fraud_predictions
    """

    try:
        row = pd.read_sql_query(query, conn).iloc[0]
        total = int(row["total_transactions"] or 0)
        predicted_fraud = int(row["predicted_fraud_transactions"] or 0)
        return {
            "total_transactions": total,
            "predicted_fraud_transactions": predicted_fraud,
            "predicted_fraud_rate": (predicted_fraud / total * 100) if total else 0.0,
            "avg_transaction_amount": float(row["avg_transaction_amount"] or 0.0),
            "avg_fraud_probability": float((row["avg_fraud_probability"] or 0.0) * 100),
        }
    except Exception as exc:
        st.error(f"Error getting statistics: {exc}")
        return {
            "total_transactions": 0,
            "predicted_fraud_transactions": 0,
            "predicted_fraud_rate": 0.0,
            "avg_transaction_amount": 0.0,
            "avg_fraud_probability": 0.0,
        }


@st.cache_data(ttl=60)
def get_time_series_data(hours=24):
    conn = get_postgres_connection()
    if not conn:
        return pd.DataFrame()

    query = """
        SELECT
            date_trunc('hour', trans_date_trans_time::timestamp) AS hour,
            COUNT(*) AS total_count,
            SUM(CASE WHEN prediction >= 1 THEN 1 ELSE 0 END) AS predicted_fraud_count
        FROM fraud_predictions
        WHERE trans_date_trans_time::timestamp >= NOW() - (%s || ' hours')::interval
        GROUP BY hour
        ORDER BY hour
    """

    try:
        return pd.read_sql_query(query, conn, params=[hours])
    except Exception as exc:
        st.error(f"Error fetching time series data: {exc}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def get_predicted_fraud_distribution(attribute):
    conn = get_postgres_connection()
    if not conn:
        return pd.DataFrame()

    allowed_attributes = {"category", "merchant"}
    if attribute not in allowed_attributes:
        raise ValueError(f"Unsupported attribute: {attribute}")

    query = f"""
        SELECT
            {attribute},
            COUNT(*) AS total_count,
            SUM(CASE WHEN prediction >= 1 THEN 1 ELSE 0 END) AS predicted_fraud_count,
            AVG(prob_1) * 100 AS avg_fraud_probability,
            SUM(CASE WHEN prediction >= 1 THEN 1 ELSE 0 END)::float / COUNT(*) * 100 AS predicted_fraud_rate
        FROM fraud_predictions
        GROUP BY {attribute}
        ORDER BY predicted_fraud_rate DESC
    """

    try:
        return pd.read_sql_query(query, conn)
    except Exception as exc:
        st.error(f"Error fetching distribution data: {exc}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def get_amount_distribution():
    conn = get_postgres_connection()
    if not conn:
        return pd.DataFrame()

    query = """
        SELECT
            CASE
                WHEN amt < 100 THEN 'Less than $100'
                WHEN amt < 500 THEN '$100-$500'
                WHEN amt < 1000 THEN '$500-$1000'
                ELSE 'Over $1000'
            END AS amount_category,
            COUNT(*) AS total_count,
            SUM(CASE WHEN prediction >= 1 THEN 1 ELSE 0 END) AS predicted_fraud_count,
            AVG(prob_1) * 100 AS avg_fraud_probability,
            SUM(CASE WHEN prediction >= 1 THEN 1 ELSE 0 END)::float / COUNT(*) * 100 AS predicted_fraud_rate
        FROM fraud_predictions
        GROUP BY amount_category
        ORDER BY predicted_fraud_rate DESC
    """

    try:
        return pd.read_sql_query(query, conn)
    except Exception as exc:
        st.error(f"Error fetching amount distribution data: {exc}")
        return pd.DataFrame()


st.sidebar.title("Fraud Detection System")
try:
    st.sidebar.image("images/workflow.png", use_container_width=True)
except Exception:
    st.sidebar.info("Workflow image not found")

page = st.sidebar.selectbox(
    "Select Page",
    ["Overview", "Transaction Analysis", "Detailed Statistics"],
)

time_filter = st.sidebar.slider("Time window (hours)", min_value=1, max_value=72, value=24)
fraud_filter = st.sidebar.checkbox("Show predicted fraud only")

if st.sidebar.button("Refresh Data"):
    st.cache_data.clear()
    st.rerun()

conn = get_postgres_connection()
if conn:
    st.sidebar.success("PostgreSQL Connected")
else:
    st.sidebar.error("PostgreSQL Connection Failed")


if page == "Overview":
    st.title("Fraud Detection Overview")

    stats = get_statistics()
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("Total Transactions", f"{stats['total_transactions']:,}")
    with col2:
        st.metric("Predicted Fraud", f"{stats['predicted_fraud_transactions']:,}")
    with col3:
        st.metric("Predicted Fraud Rate", f"{stats['predicted_fraud_rate']:.2f}%")
    with col4:
        st.metric("Average Amount", f"${stats['avg_transaction_amount']:,.2f}")
    with col5:
        st.metric("Average Fraud Probability", f"{stats['avg_fraud_probability']:.2f}%")

    st.subheader(f"Transaction Trends (Last {time_filter} hours)")
    time_data = get_time_series_data(time_filter)
    if not time_data.empty:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=time_data["hour"],
            y=time_data["total_count"],
            name="All transactions",
            line=dict(color="#2563eb", width=2),
        ))
        fig.add_trace(go.Scatter(
            x=time_data["hour"],
            y=time_data["predicted_fraud_count"],
            name="Predicted fraud",
            line=dict(color="#dc2626", width=2),
        ))
        fig.update_layout(xaxis_title="Time", yaxis_title="Transactions", height=400)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No time series data available.")

    st.subheader("Recent Transactions")
    transactions = get_transactions(limit=20, fraud_only=fraud_filter)
    if not transactions.empty:
        display_df = transactions[[
            "trans_num", "trans_date_trans_time", "cc_num", "amt", "merchant",
            "category", "fraud_probability", "status"
        ]].rename(columns={
            "trans_num": "Transaction Number",
            "trans_date_trans_time": "Time",
            "cc_num": "Credit Card Number",
            "amt": "Amount",
            "merchant": "Merchant",
            "category": "Category",
            "fraud_probability": "Fraud Probability (%)",
            "status": "Status",
        })
        st.dataframe(display_df, use_container_width=True)
    else:
        st.info("No transactions to display.")


elif page == "Transaction Analysis":
    st.title("Transaction Analysis")

    with st.expander("Search transactions", expanded=True):
        col1, col2 = st.columns(2)
        with col1:
            cc_num = st.text_input("Credit Card Number")
            merchant = st.text_input("Merchant")
        with col2:
            amount_min = st.number_input("Minimum Amount", min_value=0.0, value=0.0)
            amount_max = st.number_input("Maximum Amount", min_value=0.0, value=1000000.0)
        search_fraud = st.checkbox("Search predicted fraud only")
        search_button = st.button("Search")

    if search_button:
        conn = get_postgres_connection()
        if conn:
            query = """
                SELECT trans_num, trans_date_trans_time, cc_num, amt, merchant, category,
                       is_fraud, prediction, prob_0, prob_1
                FROM fraud_predictions
                WHERE 1=1
            """
            params = []

            if cc_num:
                query += " AND cc_num = %s"
                params.append(cc_num)
            if merchant:
                query += " AND merchant ILIKE %s"
                params.append(f"%{merchant}%")
            if amount_min > 0:
                query += " AND amt >= %s"
                params.append(amount_min)
            if amount_max < 1000000.0:
                query += " AND amt <= %s"
                params.append(amount_max)
            if search_fraud:
                query += " AND prediction >= 1"

            query += " ORDER BY trans_date_trans_time DESC LIMIT 100"

            try:
                search_results = add_status_columns(pd.read_sql_query(query, conn, params=params))
                if not search_results.empty:
                    st.subheader(f"Search Results: {len(search_results)} transactions found")
                    st.dataframe(
                        search_results[[
                            "trans_num", "trans_date_trans_time", "cc_num", "amt",
                            "merchant", "category", "fraud_probability", "status"
                        ]],
                        use_container_width=True,
                    )
                else:
                    st.info("No transactions match your search criteria.")
            except Exception as exc:
                st.error(f"Error executing search: {exc}")

    st.subheader("Latest Transactions")
    transactions = get_transactions(limit=50, fraud_only=fraud_filter)
    if not transactions.empty:
        st.dataframe(
            transactions[[
                "trans_num", "trans_date_trans_time", "cc_num", "amt",
                "merchant", "category", "fraud_probability", "status"
            ]],
            use_container_width=True,
        )

        selected_id = st.selectbox("Select a transaction number", transactions["trans_num"].tolist())
        if selected_id:
            transaction_detail = transactions[transactions["trans_num"] == selected_id].iloc[0]
            st.subheader(f"Transaction Details: {selected_id}")
            col1, col2 = st.columns(2)
            items = list(transaction_detail.items())
            with col1:
                for key, value in items[::2]:
                    st.text(f"{key}: {value}")
            with col2:
                for key, value in items[1::2]:
                    st.text(f"{key}: {value}")

            if transaction_detail["fraud_signal"] == 1:
                st.error("This transaction is currently flagged as predicted fraud.")
                st.warning(f"Fraud probability: {transaction_detail['fraud_probability']:.2f}%")
    else:
        st.info("No transactions available.")


elif page == "Detailed Statistics":
    st.title("Detailed Fraud Statistics")

    time_data = get_time_series_data(time_filter)
    if not time_data.empty:
        time_data["predicted_fraud_rate"] = (
            time_data["predicted_fraud_count"] / time_data["total_count"] * 100
        ).fillna(0)
        fig = px.line(
            time_data,
            x="hour",
            y="predicted_fraud_rate",
            title=f"Predicted Fraud Rate (Last {time_filter} hours)",
            labels={"hour": "Time", "predicted_fraud_rate": "Predicted Fraud Rate (%)"},
        )
        fig.update_traces(line_color="#dc2626")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No time series data available.")

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Predicted Fraud Distribution by Category")
        distribution = get_predicted_fraud_distribution("category")
        if not distribution.empty:
            fig = px.bar(
                distribution.head(15),
                x="category",
                y="predicted_fraud_rate",
                color="avg_fraud_probability",
                title="Top Categories by Predicted Fraud Rate",
                labels={
                    "category": "Category",
                    "predicted_fraud_rate": "Predicted Fraud Rate (%)",
                    "avg_fraud_probability": "Avg Fraud Probability (%)",
                },
                color_continuous_scale="Reds",
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No category data available.")

    with col2:
        st.subheader("Predicted Fraud Distribution by Amount Range")
        amount_distribution = get_amount_distribution()
        if not amount_distribution.empty:
            fig = px.bar(
                amount_distribution,
                x="amount_category",
                y="predicted_fraud_rate",
                color="avg_fraud_probability",
                title="Predicted Fraud Rate by Amount Range",
                labels={
                    "amount_category": "Amount Range",
                    "predicted_fraud_rate": "Predicted Fraud Rate (%)",
                    "avg_fraud_probability": "Avg Fraud Probability (%)",
                },
                color_continuous_scale="Reds",
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No amount range data available.")

    conn = get_postgres_connection()
    if conn:
        try:
            heatmap_query = """
                SELECT
                    EXTRACT(DOW FROM trans_date_trans_time::timestamp) AS day_of_week,
                    EXTRACT(HOUR FROM trans_date_trans_time::timestamp) AS hour_of_day,
                    COUNT(*) AS total_count,
                    SUM(CASE WHEN prediction >= 1 THEN 1 ELSE 0 END) AS predicted_fraud_count,
                    SUM(CASE WHEN prediction >= 1 THEN 1 ELSE 0 END)::float / COUNT(*) * 100 AS predicted_fraud_rate
                FROM fraud_predictions
                GROUP BY day_of_week, hour_of_day
                ORDER BY day_of_week, hour_of_day
            """
            heatmap_data = pd.read_sql_query(heatmap_query, conn)
            if not heatmap_data.empty:
                pivot_data = heatmap_data.pivot(index="day_of_week", columns="hour_of_day", values="predicted_fraud_rate")
                day_names = {
                    0: "Sunday", 1: "Monday", 2: "Tuesday", 3: "Wednesday",
                    4: "Thursday", 5: "Friday", 6: "Saturday",
                }
                pivot_data.index = [day_names.get(day, day) for day in pivot_data.index]
                fig = go.Figure(data=go.Heatmap(
                    z=pivot_data.values,
                    x=pivot_data.columns,
                    y=pivot_data.index,
                    colorscale="Reds",
                    hovertemplate="Hour: %{x}<br>Day: %{y}<br>Predicted fraud rate: %{z:.2f}%<extra></extra>",
                ))
                fig.update_layout(
                    title="Predicted Fraud Heatmap by Hour and Day",
                    xaxis_title="Hour of Day",
                    yaxis_title="Day of Week",
                    height=500,
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No heatmap data available.")
        except Exception as exc:
            st.error(f"Error generating heatmap: {exc}")

    stats = get_statistics()
    summary_col1, summary_col2 = st.columns(2)
    with summary_col1:
        st.metric("Total transactions", f"{stats['total_transactions']:,}")
        st.metric("Predicted fraud", f"{stats['predicted_fraud_transactions']:,}")
    with summary_col2:
        st.metric("Predicted fraud rate", f"{stats['predicted_fraud_rate']:.2f}%")
        st.metric("Average fraud probability", f"{stats['avg_fraud_probability']:.2f}%")

    try:
        merchant_distribution = get_predicted_fraud_distribution("merchant")
        if not merchant_distribution.empty:
            st.subheader("Top 10 Merchants by Predicted Fraud Rate")
            fig = px.bar(
                merchant_distribution.head(10),
                x="merchant",
                y="predicted_fraud_rate",
                hover_data=["total_count", "predicted_fraud_count", "avg_fraud_probability"],
                labels={
                    "merchant": "Merchant",
                    "predicted_fraud_rate": "Predicted Fraud Rate (%)",
                    "total_count": "Total Transactions",
                    "predicted_fraud_count": "Predicted Fraud Count",
                    "avg_fraud_probability": "Avg Fraud Probability (%)",
                },
                color="avg_fraud_probability",
                color_continuous_scale="Reds",
            )
            st.plotly_chart(fig, use_container_width=True)
    except Exception as exc:
        st.error(f"Error fetching merchant statistics: {exc}")

    if st.button("Download CSV Report"):
        conn = get_postgres_connection()
        if conn:
            try:
                report_query = """
                    SELECT *
                    FROM fraud_predictions
                    WHERE trans_date_trans_time::timestamp >= NOW() - INTERVAL '24 hours'
                    ORDER BY trans_date_trans_time DESC
                """
                report_data = pd.read_sql_query(report_query, conn)
                if not report_data.empty:
                    csv = report_data.to_csv(index=False)
                    st.download_button(
                        label="Download Full Report",
                        data=csv,
                        file_name=f"fraud_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                    )
                else:
                    st.info("No data available for report.")
            except Exception as exc:
                st.error(f"Error generating report: {exc}")


st.markdown("---")
st.markdown("Real-Time Fraud Detection System")
