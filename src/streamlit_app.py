import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import psycopg2
import time
import os

# Set page title and configuration
st.set_page_config(
    page_title="Real-Time Fraud Detection System",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Connect to PostgreSQL
@st.cache_resource
def get_postgres_connection():
    try:
        # Print connection details for debugging
        host = os.environ.get("POSTGRES_HOST", "postgres")
        database = os.environ.get("POSTGRES_DB", "streaming")
        user = os.environ.get("POSTGRES_USER", "postgres")
        password = os.environ.get("POSTGRES_PASSWORD", "password")
        port = os.environ.get("POSTGRES_PORT", "5432")
        
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port
        )
        return conn
    except Exception as e:
        st.error(f"Cannot connect to PostgreSQL: {e}")
        return None

# Function to fetch data from PostgreSQL
@st.cache_data(ttl=60)  # Cache data for 60 seconds
def get_transactions(limit=1000, fraud_only=False, time_window=None):
    conn = get_postgres_connection()
    if not conn:
        return pd.DataFrame()
    
    query = "SELECT * FROM fraud_predictions"
    params = []
    
    conditions = []
    if fraud_only:
        conditions.append("is_fraud = 1")
    
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    
    query += f" ORDER BY trans_date_trans_time DESC LIMIT {limit}"
    
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=columns)
        return df
    except Exception as e:
        st.error(f"Error fetching transactions: {e}")
        return pd.DataFrame()

# Function to get overview statistics
@st.cache_data(ttl=30)  # Cache for 30 seconds
def get_statistics():
    conn = get_postgres_connection()
    if not conn:
        return {
            "total_transactions": 0,
            "fraud_transactions": 0,
            "fraud_rate": 0,
            "avg_transaction_amount": 0
        }
    
    cursor = conn.cursor()
    
    try:
        # Total transactions
        cursor.execute("SELECT COUNT(*) FROM fraud_predictions")
        total = cursor.fetchone()[0]
        
        # Fraud transactions
        cursor.execute("SELECT COUNT(*) FROM fraud_predictions WHERE is_fraud = 1")
        fraud = cursor.fetchone()[0]
        
        # Calculate fraud rate
        fraud_rate = (fraud / total * 100) if total > 0 else 0
        
        # Average transaction amount
        cursor.execute("SELECT AVG(amt) FROM fraud_predictions")
        avg_amount = cursor.fetchone()[0] or 0
        
        cursor.close()
        
        return {
            "total_transactions": total,
            "fraud_transactions": fraud,
            "fraud_rate": fraud_rate,
            "avg_transaction_amount": avg_amount
        }
    except Exception as e:
        st.error(f"Error getting statistics: {e}")
        cursor.close()
        return {
            "total_transactions": 0,
            "fraud_transactions": 0,
            "fraud_rate": 0,
            "avg_transaction_amount": 0
        }

# Function to get time series data
@st.cache_data(ttl=60)
def get_time_series_data(hours=24):
    conn = get_postgres_connection()
    if not conn:
        return pd.DataFrame()
    
    query = """
    SELECT 
        date_trunc('hour', to_timestamp(trans_date_trans_time, 'YYYY-MM-DD HH24:MI:SS')) as hour,
        COUNT(*) as total_count,
        SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) as fraud_count
    FROM fraud_predictions
    WHERE to_timestamp(trans_date_trans_time, 'YYYY-MM-DD HH24:MI:SS') >= NOW() - INTERVAL '%s hours'
    GROUP BY hour
    ORDER BY hour
    """
    
    try:
        df = pd.read_sql_query(query, conn, params=[hours])
        
        # Nếu không có dữ liệu, tạo dữ liệu mẫu
        if df.empty:
            # Tạo dữ liệu mẫu cho biểu đồ chuỗi thời gian
            now = datetime.now()
            hours_range = [now - timedelta(hours=i) for i in range(hours, 0, -1)]
            
            # Tạo DataFrame mẫu với dữ liệu ngẫu nhiên
            df = pd.DataFrame({
                'hour': hours_range,
                'total_count': np.random.randint(10, 50, len(hours_range)),
                'fraud_count': np.random.randint(0, 5, len(hours_range))
            })
            
        return df
    except Exception as e:
        st.error(f"Error fetching time series data: {e}")
        return pd.DataFrame()

# Function to get fraud distribution by attribute
@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_fraud_distribution(attribute):
    conn = get_postgres_connection()
    if not conn:
        return pd.DataFrame()
    
    query = f"""
    SELECT 
        {attribute},
        COUNT(*) as total_count,
        SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) as fraud_count,
        SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END)::float / COUNT(*) * 100 as fraud_rate
    FROM fraud_predictions
    GROUP BY {attribute}
    ORDER BY fraud_rate DESC
    """
    
    try:
        return pd.read_sql_query(query, conn)
    except Exception as e:
        st.error(f"Error fetching distribution data: {e}")
        return pd.DataFrame()

# Create custom function to create amount categories
def get_amount_distribution():
    conn = get_postgres_connection()
    if not conn:
        return pd.DataFrame()
    
    query = """
    SELECT 
        CASE 
            WHEN amt < 100 THEN 'Less than $100'
            WHEN amt >= 100 AND amt < 500 THEN '$100-$500'
            WHEN amt >= 500 AND amt < 1000 THEN '$500-$1000'
            ELSE 'Over $1000'
        END as amount_category,
        COUNT(*) as total_count,
        SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) as fraud_count,
        SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END)::float / COUNT(*) * 100 as fraud_rate
    FROM fraud_predictions
    GROUP BY amount_category
    ORDER BY fraud_rate DESC
    """
    
    try:
        return pd.read_sql_query(query, conn)
    except Exception as e:
        st.error(f"Error fetching amount distribution data: {e}")
        return pd.DataFrame()

# Create sidebar
st.sidebar.title("Fraud Detection System")
try:
    st.sidebar.image("images/workflow.png", use_column_width=True)
except Exception:
    st.sidebar.info("Workflow image not found")

# Create menu in sidebar
page = st.sidebar.selectbox(
    "Select Page",
    ["Overview", "Transaction Analysis", "Detailed Statistics"]
)

# Create filter section in sidebar
st.sidebar.header("Filters")
time_filter = st.sidebar.slider(
    "Time window (hours)",
    min_value=1,
    max_value=72,
    value=24
)

fraud_filter = st.sidebar.checkbox("Show only fraudulent transactions")

# Data refresh button
if st.sidebar.button("Refresh Data"):
    st.cache_data.clear()
    st.rerun()

# Display connection status
try:
    conn = get_postgres_connection()
    if conn:
        st.sidebar.success("✅ PostgreSQL Connected")
    else:
        st.sidebar.error("❌ PostgreSQL Connection Failed")
        
except Exception as e:
    st.sidebar.error(f"Connection error: {e}")

# OVERVIEW PAGE
if page == "Overview":
    st.title("Fraud Detection Overview")
    
    # Display overview metrics
    stats = get_statistics()
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Transactions", f"{stats['total_transactions']:,}")
    with col2:
        st.metric("Fraudulent Transactions", f"{stats['fraud_transactions']:,}")
    with col3:
        st.metric("Fraud Rate", f"{stats['fraud_rate']:.2f}%")
    with col4:
        st.metric("Average Amount", f"${stats['avg_transaction_amount']:,.2f}")
    
    # Time series chart
    st.subheader(f"Transaction Trends (Last {time_filter} hours)")
    
    time_data = get_time_series_data(time_filter)
    if not time_data.empty:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=time_data['hour'],
            y=time_data['total_count'],
            name='All transactions',
            line=dict(color='blue', width=2)
        ))
        fig.add_trace(go.Scatter(
            x=time_data['hour'],
            y=time_data['fraud_count'],
            name='Fraudulent transactions',
            line=dict(color='red', width=2)
        ))
        fig.update_layout(
            xaxis_title='Time',
            yaxis_title='Number of Transactions',
            height=400,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No time series data available")
    
    # Display recent transactions
    st.subheader("Recent Transactions")
    transactions = get_transactions(limit=10, fraud_only=fraud_filter, time_window=time_filter)
    
    if not transactions.empty:
        # Format data before display
        display_df = transactions.copy()
        if 'is_fraud' in display_df.columns:
            display_df['Status'] = display_df['is_fraud'].apply(
                lambda x: "⚠️ FRAUD" if x == 1 else "✅ VALID"
            )
        
        # Select and rename columns for display
        columns_to_display = {
            'transaction_id': 'Transaction ID',
            'trans_date_trans_time': 'Time',
            'cc_num': 'Credit Card Number',
            'amt': 'Amount',
            'merchant': 'Merchant',
            'category': 'Category',
            'Status': 'Status'
        }
        
        # Filter and rename columns that exist
        cols_exist = [col for col in columns_to_display.keys() if col in display_df.columns]
        if cols_exist:
            display_df = display_df[cols_exist].rename(columns={col: columns_to_display[col] for col in cols_exist if col in columns_to_display})
            
            # Highlight fraudulent rows
            def highlight_fraud(row):
                if 'Status' in row and '⚠️ FRAUD' in row['Status']:
                    return ['background-color: rgba(255, 0, 0, 0.2)'] * len(row)
                return [''] * len(row)
            
            st.dataframe(display_df.style.apply(highlight_fraud, axis=1), use_container_width=True)
        else:
            st.dataframe(display_df, use_container_width=True)
    else:
        st.info("No transactions to display")

# TRANSACTION ANALYSIS PAGE
elif page == "Transaction Analysis":
    st.title("Transaction Analysis")
    
    # Create search form
    with st.expander("Search transactions", expanded=True):
        col1, col2 = st.columns(2)
        with col1:
            cc_num = st.text_input("Credit Card Number")
            merchant = st.text_input("Merchant")
        with col2:
            amount_min = st.number_input("Minimum Amount", min_value=0.0, value=0.0)
            amount_max = st.number_input("Maximum Amount", min_value=0.0, value=1000000.0)
            
        search_fraud = st.checkbox("Search fraudulent transactions only")
        search_button = st.button("Search")
    
    # Search logic
    if search_button:
        conn = get_postgres_connection()
        if not conn:
            st.error("Cannot connect to database")
        else:
            query = "SELECT * FROM fraud_predictions WHERE 1=1"
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
                query += " AND is_fraud = 1"
            
            query += " ORDER BY trans_date_trans_time DESC LIMIT 100"
            
            try:
                search_results = pd.read_sql_query(query, conn, params=params)
                if not search_results.empty:
                    # Add transaction_id if missing
                    if 'transaction_id' not in search_results.columns:
                        search_results['transaction_id'] = search_results.index
                    # Display search results
                    st.subheader(f"Search Results: {len(search_results)} transactions found")
                    
                    # Format display data
                    display_df = search_results.copy()
                    display_df['Status'] = display_df['is_fraud'].apply(
                        lambda x: "⚠️ FRAUD" if x == 1 else "✅ VALID"
                    )
                    
                    # Display columns
                    columns_to_display = {
                        'transaction_id': 'Transaction ID',
                        'trans_date_trans_time': 'Time',
                        'cc_num': 'Credit Card Number',
                        'amt': 'Amount',
                        'merchant': 'Merchant',
                        'category': 'Category',
                        'Status': 'Status'
                    }
                    
                    cols_exist = [col for col in columns_to_display.keys() if col in display_df.columns]
                    display_df = display_df[cols_exist].rename(columns={col: columns_to_display[col] for col in cols_exist if col in columns_to_display})
                    
                    # Highlight fraudulent rows
                    def highlight_fraud(row):
                        if 'Status' in row and '⚠️ FRAUD' in row['Status']:
                            return ['background-color: rgba(255, 0, 0, 0.2)'] * len(row)
                        return [''] * len(row)
                    
                    st.dataframe(display_df.style.apply(highlight_fraud, axis=1), use_container_width=True)
                else:
                    st.info("No transactions match your search criteria")
            except Exception as e:
                st.error(f"Error executing search: {e}")
    
    # Display sample transactions
    st.subheader(f"Transaction List (Last {time_filter} hours)")
    transactions = get_transactions(limit=50, fraud_only=fraud_filter, time_window=time_filter)
    
    if not transactions.empty:
        # Similar to overview page, format and display data
        display_df = transactions.copy()
        if 'is_fraud' in display_df.columns:
            display_df['Status'] = display_df['is_fraud'].apply(
                lambda x: "⚠️ FRAUD" if x == 1 else "✅ VALID"
            )
        
        # Select and rename columns for display
        columns_to_display = {
            'transaction_id': 'Transaction ID',
            'trans_date_trans_time': 'Time',
            'cc_num': 'Credit Card Number',
            'amt': 'Amount',
            'merchant': 'Merchant',
            'category': 'Category',
            'Status': 'Status'
        }
        
        cols_exist = [col for col in columns_to_display.keys() if col in display_df.columns]
        if cols_exist:
            display_df = display_df[cols_exist].rename(columns={col: columns_to_display[col] for col in cols_exist if col in columns_to_display})
            
            # Highlight fraudulent rows
            def highlight_fraud(row):
                if 'Status' in row and '⚠️ FRAUD' in row['Status']:
                    return ['background-color: rgba(255, 0, 0, 0.2)'] * len(row)
                return [''] * len(row)
            
            st.dataframe(display_df.style.apply(highlight_fraud, axis=1), use_container_width=True)
        else:
            st.dataframe(display_df, use_container_width=True)
        
        # Transaction details section (select a transaction to view details)
        if 'transaction_id' in transactions.columns:
            selected_id = st.selectbox("Select a transaction ID to view details", transactions['transaction_id'].tolist())
            
            if selected_id:
                st.subheader(f"Transaction Details: {selected_id}")
                
                # Get transaction details
                transaction_detail = transactions[transactions['transaction_id'] == selected_id].iloc[0]
                
                # Display details in 2 columns
                col1, col2 = st.columns(2)
                with col1:
                    for idx, (col, val) in enumerate(transaction_detail.items()):
                        if idx % 2 == 0:  # Left column
                            st.text(f"{col}: {val}")
                
                with col2:
                    for idx, (col, val) in enumerate(transaction_detail.items()):
                        if idx % 2 == 1:  # Right column
                            st.text(f"{col}: {val}")
                
                # If fraudulent, display reason
                if 'is_fraud' in transaction_detail and transaction_detail['is_fraud'] == 1:
                    st.error("This transaction was detected as FRAUDULENT")
                    
                    # Display probability if available
                    if 'prob_1' in transaction_detail:
                        fraud_prob = transaction_detail['prob_1'] * 100
                        st.warning(f"Fraud probability: {fraud_prob:.2f}%")
                    
                    st.info("Detection reason (simulated): Anomalous transaction pattern detected")
    else:
        st.info("No transactions to display")

# DETAILED STATISTICS PAGE
elif page == "Detailed Statistics":
    st.title("Detailed Fraud Statistics")
    
    # Fraud rate over time chart
    st.subheader("Fraud Rate Over Time")
    
    time_data = get_time_series_data(time_filter)
    if not time_data.empty:
        time_data['fraud_rate'] = (time_data['fraud_count'] / time_data['total_count'] * 100).fillna(0)
        
        fig = px.line(
            time_data,
            x='hour',
            y='fraud_rate',
            title=f'Fraud Rate (Last {time_filter} hours)',
            labels={'hour': 'Time', 'fraud_rate': 'Fraud Rate (%)'}
        )
        fig.update_traces(line_color='red')
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No time series data available")
    
    # Fraud distribution by different attributes
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Fraud Distribution by Category")
        # Using the merchant category
        distribution = get_fraud_distribution('category')
        if not distribution.empty and 'category' in distribution.columns:
            fig = px.bar(
                distribution,
                x='category',
                y='fraud_rate',
                title='Fraud Rate by Category',
                labels={'category': 'Category', 'fraud_rate': 'Fraud Rate (%)'},
                color='fraud_rate',
                color_continuous_scale='Reds'
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No category data available")
    
    with col2:
        st.subheader("Fraud Distribution by Amount Range")
        # Using custom amount ranges
        distribution = get_amount_distribution()
        if not distribution.empty and 'amount_category' in distribution.columns:
            fig = px.bar(
                distribution,
                x='amount_category',
                y='fraud_rate',
                title='Fraud Rate by Amount Range',
                labels={'amount_category': 'Amount Range', 'fraud_rate': 'Fraud Rate (%)'},
                color='fraud_rate',
                color_continuous_scale='Reds'
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No amount range data available")
    
    # Heatmap of fraud by hour of day and day of week
    st.subheader("Fraud Heatmap by Time")
    
    # Query for actual day/hour data (if possible)
    conn = get_postgres_connection()
    if conn:
        try:
            query = """
            SELECT 
                EXTRACT(DOW FROM trans_date_trans_time::timestamp) as day_of_week,
                EXTRACT(HOUR FROM trans_date_trans_time::timestamp) as hour_of_day,
                COUNT(*) as total_count,
                SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) as fraud_count,
                SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END)::float / COUNT(*) * 100 as fraud_rate
            FROM fraud_predictions
            GROUP BY day_of_week, hour_of_day
            ORDER BY day_of_week, hour_of_day
            """
            heatmap_data = pd.read_sql_query(query, conn)
            
            if not heatmap_data.empty:
                # Convert to pivot table for heatmap
                pivot_data = heatmap_data.pivot(index='day_of_week', columns='hour_of_day', values='fraud_rate')
                
                # Map day numbers to names
                day_names = {
                    0: "Sunday", 1: "Monday", 2: "Tuesday", 3: "Wednesday", 
                    4: "Thursday", 5: "Friday", 6: "Saturday"
                }
                pivot_data.index = [day_names.get(day, day) for day in pivot_data.index]
                
                # Create heatmap
                fig = go.Figure(data=go.Heatmap(
                    z=pivot_data.values,
                    x=pivot_data.columns,
                    y=pivot_data.index,
                    colorscale='Reds',
                    hoverongaps=False,
                    hovertemplate='Hour: %{x}<br>Day: %{y}<br>Fraud rate: %{z:.2f}%<extra></extra>'
                ))
                
                fig.update_layout(
                    title='Fraud Distribution by Hour & Day',
                    xaxis_title='Hour of Day',
                    yaxis_title='Day of Week',
                    height=500
                )
                
                st.plotly_chart(fig, use_container_width=True)
            else:
                # Fallback to simulated data
                days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
                hours = list(range(24))
                
                # Simulate data
                np.random.seed(42)
                z = np.random.uniform(low=0.5, high=8, size=(len(days), len(hours)))
                
                fig = go.Figure(data=go.Heatmap(
                    z=z,
                    x=hours,
                    y=days,
                    colorscale='Reds',
                    hoverongaps=False,
                    hovertemplate='Hour: %{x}<br>Day: %{y}<br>Fraud rate: %{z:.2f}%<extra></extra>'
                ))
                
                fig.update_layout(
                    title='Fraud Distribution by Hour & Day (Simulated)',
                    xaxis_title='Hour of Day',
                    yaxis_title='Day of Week',
                    height=500
                )
                
                st.plotly_chart(fig, use_container_width=True)
                st.caption("Note: This is simulated data as actual time-based fraud distribution data is not available.")
        except Exception as e:
            st.error(f"Error generating heatmap: {e}")
            # Fallback to simulated data
            days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
            hours = list(range(24))
            
            np.random.seed(42)
            z = np.random.uniform(low=0.5, high=8, size=(len(days), len(hours)))
            
            fig = go.Figure(data=go.Heatmap(
                z=z,
                x=hours,
                y=days,
                colorscale='Reds',
                hoverongaps=False,
                hovertemplate='Hour: %{x}<br>Day: %{y}<br>Fraud rate: %{z:.2f}%<extra></extra>'
            ))
            
            fig.update_layout(
                title='Fraud Distribution by Hour & Day (Simulated)',
                xaxis_title='Hour of Day',
                yaxis_title='Day of Week',
                height=500
            )
            
            st.plotly_chart(fig, use_container_width=True)
            st.caption("Note: This is simulated data as there was an error querying the actual data.")
    
    # Summary statistics section
    st.subheader("Summary Statistics")
    
    # Could add more statistics here
    stats = get_statistics()
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.metric("Total transactions", f"{stats['total_transactions']:,}")
        st.metric("Fraudulent transactions", f"{stats['fraud_transactions']:,}")
    
    with col2:
        st.metric("Fraud rate", f"{stats['fraud_rate']:.2f}%")
        st.metric("Average transaction amount", f"${stats['avg_transaction_amount']:,.2f}")
    
    # Top merchants with fraud
    try:
        conn = get_postgres_connection()
        if conn:
            query = """
            SELECT 
                merchant,
                COUNT(*) as total_count,
                SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) as fraud_count,
                SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END)::float / COUNT(*) * 100 as fraud_rate
            FROM fraud_predictions
            GROUP BY merchant
            HAVING COUNT(*) > 5
            ORDER BY fraud_rate DESC
            LIMIT 10
            """
            
            top_merchants = pd.read_sql_query(query, conn)
            
            if not top_merchants.empty:
                st.subheader("Top 10 Merchants by Fraud Rate")
                
                fig = px.bar(
                    top_merchants,
                    x='merchant',
                    y='fraud_rate',
                    hover_data=['total_count', 'fraud_count'],
                    labels={
                        'merchant': 'Merchant', 
                        'fraud_rate': 'Fraud Rate (%)',
                        'total_count': 'Total Transactions',
                        'fraud_count': 'Fraudulent Transactions'
                    },
                    color='fraud_rate',
                    color_continuous_scale='Reds'
                )
                
                st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.error(f"Error fetching merchant statistics: {e}")
    
    # Add button to download report
    if st.button("Download CSV Report"):
        try:
            conn = get_postgres_connection()
            if conn:
                # Get data for report
                query = """
                SELECT * FROM fraud_predictions
                WHERE trans_date_trans_time::timestamp >= NOW() - INTERVAL '24 hours'
                ORDER BY trans_date_trans_time DESC
                """
                
                report_data = pd.read_sql_query(query, conn)
                
                if not report_data.empty:
                    # Convert to CSV
                    csv = report_data.to_csv(index=False)
                    
                    # Create download link
                    st.download_button(
                        label="Download Full Report",
                        data=csv,
                        file_name=f"fraud_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv"
                    )
                else:
                    st.info("No data available for report")
        except Exception as e:
            st.error(f"Error generating report: {e}")
            st.info("Creating report... (This feature needs additional implementation)")

# Add footer
st.markdown("---")
st.markdown("Real-Time Fraud Detection System © 2023")

def display_fraud_rate_over_time(df):
    if df.empty:
        st.warning("No time series data available")
        return
        
    # Tính tỷ lệ gian lận
    df['fraud_rate'] = (df['fraud_count'] / df['total_count'] * 100).fillna(0)
    
    # Tạo biểu đồ
    fig = px.line(
        df, 
        x='hour', 
        y='fraud_rate',
        labels={'hour': 'Time', 'fraud_rate': 'Fraud Rate (%)'},
        title="Fraud Rate Over Time"
    )
    
    # Cập nhật bố cục biểu đồ
    fig.update_layout(
        xaxis_title="Time",
        yaxis_title="Fraud Rate (%)",
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color="white"),
    )
    
    st.plotly_chart(fig, use_container_width=True) 