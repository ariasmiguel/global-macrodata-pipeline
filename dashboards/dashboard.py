"""
Streamlit dashboard for visualizing macroeconomic data.
This dashboard displays key economic indicators, historical trends, and correlations.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import json
from pathlib import Path
from datetime import datetime
import numpy as np

# Page configuration
st.set_page_config(
    page_title="Macroeconomic Data Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for styling
st.markdown("""
    <style>
    .main {
        background-color: #F9FAFB;
    }
    .stButton>button {
        background-color: #1E3A8A;
        color: white;
        border-radius: 8px;
        padding: 0.5rem 1rem;
    }
    .stButton>button:hover {
        background-color: #2563EB;
    }
    h1, h2, h3 {
        color: #1F2937;
        font-family: 'Inter', sans-serif;
    }
    </style>
    """, unsafe_allow_html=True)

def load_latest_dashboard_data():
    """Load the most recent dashboard data file."""
    data_dir = Path("data/dashboards")
    if not data_dir.exists():
        st.error("Dashboard data directory not found. Please run the data generation script first.")
        return None
    
    # Get the most recent dashboard_combined file
    dashboard_files = list(data_dir.glob("dashboard_combined_*.json"))
    if not dashboard_files:
        st.error("No dashboard data files found. Please run the data generation script first.")
        return None
    
    latest_file = max(dashboard_files, key=lambda x: x.stat().st_mtime)
    
    try:
        with open(latest_file, 'r') as f:
            data = json.load(f)
            
        # Validate data structure
        required_keys = ['key_indicators', 'historical_trends', 'commodity_comparison', 'correlation_matrix']
        missing_keys = [key for key in required_keys if key not in data]
        
        if missing_keys:
            st.warning(f"Missing data sections: {', '.join(missing_keys)}")
        
        # Check for empty sections
        empty_sections = [key for key in required_keys if key in data and not data[key]]
        if empty_sections:
            st.warning(f"Empty data sections: {', '.join(empty_sections)}")
        
        # Add timestamp if missing
        if 'timestamp' not in data:
            data['timestamp'] = datetime.now().isoformat()
            
        return data
        
    except json.JSONDecodeError as e:
        st.error(f"Error parsing dashboard data: Invalid JSON format in {latest_file}")
        return None
    except Exception as e:
        st.error(f"Error loading dashboard data: {str(e)}")
        return None

def render_key_indicators(data):
    """Render the key indicators section."""
    st.header("Key Economic Indicators")
    
    indicators = data.get('key_indicators', {})
    if not indicators:
        st.info("No key indicators data available.")
        return
    
    # Iterate through categories
    for category, subcategories in indicators.items():
        st.subheader(category.title())
        
        # Count total indicators for column layout
        total_indicators = sum(len(indicators) for indicators in subcategories.values())
        if total_indicators == 0:
            continue
        
        # Create columns for each subcategory
        for subcategory, indicators in subcategories.items():
            if not indicators:
                continue
                
            st.markdown(f"**{subcategory}**")
            
            # Create columns for indicators (max 3 per row)
            num_indicators = len(indicators)
            num_cols = min(3, num_indicators)
            cols = st.columns(num_cols)
            
            # Display indicators in columns
            for i, (raw_id, info) in enumerate(indicators.items()):
                with cols[i % num_cols]:
                    try:
                        value = float(info.get('value', 0))
                        mom_change = float(info.get('mom_change', 0)) if info.get('mom_change') is not None else None
                        yoy_change = float(info.get('yoy_change', 0)) if info.get('yoy_change') is not None else None
                        
                        # Create a container for the indicator
                        with st.container():
                            # Display the main value with series name
                            st.metric(
                                label=info.get('series_name', raw_id),
                                value=f"{value:.2f}",
                                delta=None
                            )
                            
                            # Display MoM and YoY changes in smaller text
                            if mom_change is not None or yoy_change is not None:
                                changes_text = []
                                if mom_change is not None:
                                    changes_text.append(f"MoM: {mom_change:.2f}%")
                                if yoy_change is not None:
                                    changes_text.append(f"YoY: {yoy_change:.2f}%")
                                st.markdown(f"<div style='font-size: 0.8em; color: #666;'>{' | '.join(changes_text)}</div>", unsafe_allow_html=True)
                                
                            # Display series ID in smaller text
                            st.markdown(f"<div style='font-size: 0.7em; color: #999;'>ID: {raw_id}</div>", unsafe_allow_html=True)
                                
                    except (ValueError, TypeError) as e:
                        st.warning(f"Invalid data for indicator {raw_id}: {str(e)}")

def render_historical_trends(data):
    """Render the historical trends section."""
    st.header("Historical Trends")
    
    # Get the historical trends data
    trends = data.get('historical_trends', {})
    
    if not trends:
        st.info("No historical trends data available.")
        return
    
    # Add category selector
    categories = list(trends.keys())
    if not categories:
        return
        
    selected_category = st.selectbox(
        "Select Category",
        categories,
        format_func=lambda x: x.title()
    )
    
    category_data = trends.get(selected_category, {})
    if not category_data:
        st.info(f"No data available for category: {selected_category}")
        return
    
    # Create a DataFrame for plotting
    plot_data = []
    for subcategory, subcategory_data in category_data.items():
        for raw_id, series_info in subcategory_data.items():
            if 'data_points' not in series_info:
                continue
            for point in series_info['data_points']:
                plot_data.append({
                    'date': point['date'],
                    'value': point['value'],
                    'series': series_info.get('series_name', raw_id),
                    'subcategory': subcategory,
                    'raw_id': raw_id
                })
    
    if plot_data:
        df = pd.DataFrame(plot_data)
        
        # Create the line chart
        fig = px.line(
            df,
            x='date',
            y='value',
            color='series',
            facet_col='subcategory',
            title=f'{selected_category.title()} Indicators Over Time',
            template='plotly_white',
            hover_data=['raw_id']  # Show raw_id in tooltip
        )
        
        # Update layout
        fig.update_layout(
            plot_bgcolor='white',
            paper_bgcolor='white',
            font=dict(family='Inter'),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )
        
        st.plotly_chart(fig, use_container_width=True)

def render_correlation_matrix(data):
    """Render the correlation matrix section."""
    st.header("Correlation Matrix")
    
    # Get the correlation matrices data
    matrices = data.get('correlation_matrices', {})
    
    if not matrices:
        st.info("No correlation data available.")
        return
    
    # Add category selector
    categories = list(matrices.keys())
    if not categories:
        return
        
    selected_category = st.selectbox(
        "Select Category for Correlation Analysis",
        categories,
        format_func=lambda x: x.title()
    )
    
    corr_matrix = matrices.get(selected_category, {})
    if not corr_matrix:
        st.info(f"No correlation data available for category: {selected_category}")
        return
    
    # Create a DataFrame for the correlation matrix
    matrix_data = []
    for series1, correlations in corr_matrix.items():
        for series2, value in correlations.items():
            matrix_data.append({
                'Series 1': series1,
                'Series 2': series2,
                'Correlation': value
            })
    
    if matrix_data:
        df = pd.DataFrame(matrix_data)
        
        # Create the heatmap
        fig = go.Figure(data=go.Heatmap(
            z=df.pivot('Series 1', 'Series 2', 'Correlation').values,
            x=df['Series 2'].unique(),
            y=df['Series 1'].unique(),
            colorscale='RdBu',
            zmid=0,
            text=np.round(df.pivot('Series 1', 'Series 2', 'Correlation').values, 2),
            texttemplate='%{text}',
            textfont={"size": 10}
        ))
        
        # Update layout
        fig.update_layout(
            title=f'Correlation Matrix - {selected_category.title()}',
            xaxis_title='',
            yaxis_title='',
            height=600,
            template='plotly_white',
            font=dict(family='Inter')
        )
        
        st.plotly_chart(fig, use_container_width=True)

def render_commodity_comparison(data):
    """Render the commodity comparison section."""
    st.header("Commodity Price Comparison")
    
    # Get the commodity comparison data
    commodities = data.get('commodity_comparison', {})
    
    if commodities:
        # Create a DataFrame for plotting
        plot_data = []
        for series_id, info in commodities.items():
            plot_data.append({
                'Indicator': info['indicator_name'],
                'Value': info['value'],
                'YoY Change': info['yoy_change']
            })
        
        df = pd.DataFrame(plot_data)
        
        # Create the bar chart
        fig = go.Figure(data=[
            go.Bar(
                x=df['Indicator'],
                y=df['Value'],
                text=df['YoY Change'].apply(lambda x: f"{x:.2f}%" if x else "N/A"),
                textposition='auto',
                marker_color='#1E3A8A'
            )
        ])
        
        # Update layout
        fig.update_layout(
            title='Commodity Price Levels and Year-over-Year Changes',
            xaxis_title='',
            yaxis_title='Price Level',
            template='plotly_white',
            font=dict(family='Inter'),
            showlegend=False
        )
        
        st.plotly_chart(fig, use_container_width=True)

def main():
    """Main dashboard function."""
    # Title and description
    st.title("Macroeconomic Data Dashboard")
    st.markdown("""
        This dashboard provides real-time visualization of key economic indicators,
        historical trends, and correlations between different economic metrics.
    """)
    
    # Load the dashboard data
    data = load_latest_dashboard_data()
    if not data:
        return
    
    # Add a sidebar with filters
    with st.sidebar:
        st.header("Filters")
        st.markdown("---")
        
        # Date range filter
        st.subheader("Date Range")
        date_range = st.date_input(
            "Select Date Range",
            value=(datetime.now().date(), datetime.now().date()),
            max_value=datetime.now().date()
        )
        
        # Category filter
        st.subheader("Categories")
        categories = ["All", "Commodity", "Labor", "Financial"]
        selected_category = st.selectbox("Select Category", categories)
        
        st.markdown("---")
        st.markdown("""
            <div style='font-size: 0.8em; color: #6B7280;'>
                Data last updated: {timestamp}
            </div>
        """.format(timestamp=data.get('timestamp', 'N/A')), unsafe_allow_html=True)
    
    # Render the dashboard sections
    render_key_indicators(data)
    render_historical_trends(data)
    
    # Create two columns for the bottom charts
    col1, col2 = st.columns(2)
    
    with col1:
        render_correlation_matrix(data)
    
    with col2:
        render_commodity_comparison(data)
    
    # Add a footer with data source information
    st.markdown("---")
    st.markdown("""
        <div style='font-size: 0.8em; color: #6B7280; text-align: center;'>
            Data source: U.S. Bureau of Labor Statistics (BLS) API
        </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main() 