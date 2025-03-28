from typing import List, Dict, Optional
import pandas as pd
import plotly.graph_objects as go
from loguru import logger
from pathlib import Path
import os
from dotenv import load_dotenv
from clickhouse_driver import Client

# Load environment variables
load_dotenv()

class SupplyChainPressureAnalyzer:
    """Analyzes and visualizes supply chain price pressures using BLS data from ClickHouse."""
    
    def __init__(self):
        """Initialize the analyzer with ClickHouse connection."""
        self.client = Client(
            host=os.getenv('CLICKHOUSE_HOST', 'localhost'),
            port=int(os.getenv('CLICKHOUSE_PORT', 9000)),
            user=os.getenv('CLICKHOUSE_USER', 'default'),
            password=os.getenv('CLICKHOUSE_PASSWORD', 'clickhouse'),
            database='macro'
        )
        
        # Updated series mapping to match BLS FD-ID system stages with available series
        self.series_mapping = {
            'Stage 1': [
                'WPUID511',    # Inputs to stage 1 goods producers
                'WPUID5111',   # Inputs to stage 1 goods producers, goods
                'WPUID51111',  # Inputs to stage 1 goods producers, foods
                'WPUID51112',  # Inputs to stage 1 goods producers, energy
                'WPUID51113',  # Inputs to stage 1 goods producers, goods excluding foods and energy
            ],
            'Stage 2': [
                'WPUID521',    # Inputs to stage 2 goods producers
                'WPUID5211',   # Inputs to stage 2 goods producers, goods
                'WPUID52111',  # Inputs to stage 2 goods producers, foods
                'WPUID52112',  # Inputs to stage 2 goods producers, energy
                'WPUID52113',  # Inputs to stage 2 goods producers, goods excluding foods and energy
            ],
            'Stage 3': [
                'WPUID531',    # Inputs to stage 3 goods producers
                'WPUID5311',   # Inputs to stage 3 goods producers, goods
                'WPUID53111',  # Inputs to stage 3 goods producers, foods
                'WPUID53112',  # Inputs to stage 3 goods producers, energy
                'WPUID53113',  # Inputs to stage 3 goods producers, goods excluding foods and energy
            ],
            'Stage 4': [
                'WPUID541',    # Inputs to stage 4 goods producers
                'WPUID5411',   # Inputs to stage 4 goods producers, goods
                'WPUID54111',  # Inputs to stage 4 goods producers, foods
                'WPUID54112',  # Inputs to stage 4 goods producers, energy
                'WPUID54113',  # Inputs to stage 4 goods producers, goods excluding foods and energy
            ]
        }
        
    def get_latest_values(self) -> pd.DataFrame:
        """Get values for the latest available date."""
        query = """
        WITH latest_date AS (
            SELECT max(date) as max_date
            FROM economic_indicators_values
        ),
        stage_values AS (
            SELECT 
                m.raw_series_id,
                v.date,
                v.value,
                CASE
                    WHEN m.raw_series_id IN %(stage1)s THEN 'Stage 1'
                    WHEN m.raw_series_id IN %(stage2)s THEN 'Stage 2'
                    WHEN m.raw_series_id IN %(stage3)s THEN 'Stage 3'
                    WHEN m.raw_series_id IN %(stage4)s THEN 'Stage 4'
                END as stage
            FROM economic_indicators_values v
            JOIN economic_indicators_metadata m ON v.series_id = m.series_id
            WHERE m.raw_series_id IN %(all_series)s
                AND v.is_annual_avg = 0
        ),
        current_values AS (
            SELECT 
                stage,
                raw_series_id,
                value as current_value
            FROM stage_values
            WHERE date = (SELECT max_date FROM latest_date)
        ),
        prev_month_values AS (
            SELECT 
                stage,
                raw_series_id,
                value as prev_month_value
            FROM stage_values
            WHERE date = dateAdd(month, -1, (SELECT max_date FROM latest_date))
        ),
        prev_year_values AS (
            SELECT 
                stage,
                raw_series_id,
                value as prev_year_value
            FROM stage_values
            WHERE date = dateAdd(year, -1, (SELECT max_date FROM latest_date))
        )
        SELECT 
            cv.stage,
            toYYYYMM((SELECT max_date FROM latest_date)) as period,
            round(avg((cv.current_value - py.prev_year_value) / py.prev_year_value * 100), 1) as yoy_change,
            round(avg((cv.current_value - pm.prev_month_value) / pm.prev_month_value * 100), 1) as mom_change,
            CASE 
                WHEN avg((cv.current_value - py.prev_year_value) / py.prev_year_value * 100) <= 1.0 THEN 'Low'
                WHEN avg((cv.current_value - py.prev_year_value) / py.prev_year_value * 100) <= 2.5 THEN 'Medium'
                ELSE 'High'
            END as pressure_level
        FROM current_values cv
        JOIN prev_month_values pm ON cv.stage = pm.stage AND cv.raw_series_id = pm.raw_series_id
        JOIN prev_year_values py ON cv.stage = py.stage AND cv.raw_series_id = py.raw_series_id
        GROUP BY cv.stage
        ORDER BY cv.stage
        """
        
        # Prepare parameters for the query
        params = {
            'stage1': list(self.series_mapping['Stage 1']),
            'stage2': list(self.series_mapping['Stage 2']),
            'stage3': list(self.series_mapping['Stage 3']),
            'stage4': list(self.series_mapping['Stage 4']),
            'all_series': [s for series_list in self.series_mapping.values() for s in series_list]
        }
        
        result = self.client.execute(query, params)
        return pd.DataFrame(result, columns=['Stage', 'Period', 'YoY Change', 'MoM Change', 'Pressure Level'])

    def print_stage_comparison(self):
        """Print stage comparison table."""
        df = self.get_latest_values()
        logger.info("\nLatest Available Values:")
        logger.info("\nStage Comparison:")
        logger.info(df.to_string(index=False))
        return df

    def load_data(self) -> pd.DataFrame:
        """Load BLS data from ClickHouse."""
        logger.info("Loading data from ClickHouse")
        
        # Convert series mapping to a flat list for the query
        all_series = []
        for series_list in self.series_mapping.values():
            all_series.extend(series_list)
            
        # Query to get the data
        query = """
        SELECT 
            m.raw_series_id,
            v.date,
            v.value
        FROM economic_indicators_values v
        JOIN economic_indicators_metadata m ON v.series_id = m.series_id
        WHERE m.raw_series_id IN %(series_ids)s
            AND v.is_annual_avg = 0  -- Only use monthly values
        ORDER BY m.raw_series_id, v.date
        """
        
        # Execute query and convert to DataFrame
        result = self.client.execute(
            query,
            {'series_ids': all_series},
            settings={'use_numpy': True}
        )
        
        return pd.DataFrame(
            result,
            columns=['raw_series_id', 'date', 'value']
        )
    
    def calculate_price_pressures(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """
        Calculate price pressure indices for each stage of the supply chain.
        
        Args:
            df: DataFrame containing BLS data
            
        Returns:
            Dictionary mapping stage names to their price pressure indices
        """
        logger.info("Calculating price pressure indices")
        
        stage_data = {}
        for stage, series_ids in self.series_mapping.items():
            # Filter data for the stage's series
            stage_df = df[df['raw_series_id'].isin(series_ids)]
            
            # Calculate average price index for the stage
            stage_data[stage] = stage_df.groupby('date')['value'].mean()
            
        return stage_data
    
    def calculate_yoy_changes(self, stage_data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """Calculate year-over-year changes for each stage."""
        logger.info("Calculating year-over-year changes")
        
        yoy_changes = {}
        for stage, data in stage_data.items():
            yoy_changes[stage] = data.pct_change(periods=12) * 100
            
        return yoy_changes
    
    def plot_supply_chain_pressures(self, yoy_changes: Dict[str, pd.DataFrame]) -> go.Figure:
        """
        Create a line plot showing YoY changes in price pressures across the supply chain.
        
        Args:
            yoy_changes: Dictionary mapping stages to their YoY changes
            
        Returns:
            Plotly figure object
        """
        logger.info("Creating supply chain pressures visualization")
        
        fig = go.Figure()
        
        colors = {
            'Stage 1': '#1f77b4',
            'Stage 2': '#ff7f0e',
            'Stage 3': '#2ca02c',
            'Stage 4': '#d62728'
        }
        
        for stage, data in yoy_changes.items():
            fig.add_trace(
                go.Scatter(
                    x=data.index,
                    y=data.values,
                    name=stage,
                    line=dict(color=colors[stage]),
                    mode='lines'
                )
            )
            
        fig.update_layout(
            title='Supply Chain Price Pressures (YoY % Change)',
            xaxis_title='Date',
            yaxis_title='Year-over-Year % Change',
            template='plotly_white',
            hovermode='x unified',
            showlegend=True,
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01
            )
        )
        
        return fig

def main():
    """Main function to run the supply chain pressure analysis."""
    # Set up logging
    logger.add("logs/supply_chain_pressures.log", rotation="10 MB")
    
    try:
        # Initialize analyzer
        analyzer = SupplyChainPressureAnalyzer()
        
        # Print stage comparison
        comparison_df = analyzer.print_stage_comparison()
        
        # Load and process data
        df = analyzer.load_data()
        stage_data = analyzer.calculate_price_pressures(df)
        yoy_changes = analyzer.calculate_yoy_changes(stage_data)
        
        # Create visualization
        fig = analyzer.plot_supply_chain_pressures(yoy_changes)
        
        # Save the plot
        output_dir = Path("output/analytics")
        output_dir.mkdir(parents=True, exist_ok=True)
        fig.write_html(output_dir / "supply_chain_pressures.html")
        logger.info("Analysis completed successfully")
        
    except Exception as e:
        logger.error(f"Error in supply chain pressure analysis: {str(e)}")
        raise

if __name__ == "__main__":
    main() 