import io
import mplfinance as mpf
import pandas as pd
import matplotlib.pyplot as plt
from typing import Dict, List
import numpy as np
from .logging import get_logger

logger = get_logger(__name__)

class ChartGenerator:
    """Generate elite institutional charts for signals"""
    
    def __init__(self):
        self.style = 'yahoo'
        self.figsize = (12, 8)
    
    def create_elite_chart(self, signal: Dict) -> bytes:
        """Create elite institutional chart for signal"""
        try:
            df = signal['df'].tail(150)  # Last 150 candles
            df = df.set_index('time')
            
            # Create figure with subplots
            fig, axes = plt.subplots(2, 1, figsize=self.figsize, 
                                   gridspec_kw={'height_ratios': [3, 1]})
            
            # Main price chart
            self._plot_main_chart(axes[0], df, signal)
            
            # Volume subplot
            self._plot_volume(axes[1], df)
            
            # Add institutional annotations
            self._add_institutional_annotations(axes[0], signal)
            
            plt.tight_layout()
            
            # Save to bytes
            buf = io.BytesIO()
            plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
            buf.seek(0)
            chart_bytes = buf.getvalue()
            buf.close()
            plt.close(fig)
            
            return chart_bytes
            
        except Exception as e:
            logger.error(f"Chart generation failed: {e}")
            # Return empty bytes as fallback
            return b''

    def _plot_main_chart(self, ax, df: pd.DataFrame, signal: Dict):
        """Plot main price chart with institutional features"""
        # Candlestick plot
        mpf.plot(df, type='candle', style=self.style, ax=ax, volume=False)
        
        # Order block zone
        ob = signal['ob']
        ax.axhspan(ob.body_low, ob.body_high, alpha=0.3, color='blue', label='Order Block')
        
        # Entry and levels
        ax.axhline(signal['entry'], color='green', linestyle='--', linewidth=2, label='Entry')
        ax.axhline(signal['sl'], color='red', linestyle='-', linewidth=2, label='SL')
        
        # TP levels
        colors = ['orange', 'yellow', 'purple']
        for i, tp in enumerate(signal['tps']):
            ax.axhline(tp, color=colors[i], linestyle=':', linewidth=1.5, label=f'TP{i+1}')
        
        # FU candle highlight if present
        fu_candle = signal.get('fu_candle')
        if fu_candle:
            fu_idx = min(fu_candle.idx, len(df) - 1)
            fu_color = 'red' if fu_candle.direction == 'bearish' else 'green'
            ax.axvspan(df.index[fu_idx], df.index[fu_idx], alpha=0.5, color=fu_color, label='FU Candle')
        
        ax.set_title(f"🏆 {signal['symbol']} {signal['tf']} | {signal['side']} | Score: {signal['score']}/10")
        ax.legend()

    def _plot_volume(self, ax, df: pd.DataFrame):
        """Plot volume subplot"""
        ax.bar(df.index, df['v'], color=['green' if c > o else 'red' 
                                       for c, o in zip(df['c'], df['o'])], alpha=0.7)
        ax.set_ylabel('Volume')
        ax.grid(True, alpha=0.3)

    def _add_institutional_annotations(self, ax, signal: Dict):
        """Add institutional annotations to chart"""
        # Liquidity zones
        liquidity_zones = signal.get('liquidity_zones', [])
        for zone in liquidity_zones[:3]:  # Show top 3 zones
            color = 'red' if 'high' in zone.type else 'green'
            ax.axhline(zone.price, color=color, linestyle='--', alpha=0.5, linewidth=1)
        
        # Score breakdown text
        breakdown = signal.get('score_breakdown', {})
        score_text = "\n".join([f"{k}: {v:.1f}" for k, v in breakdown.items()])
        
        ax.text(0.02, 0.98, score_text, transform=ax.transAxes, fontsize=8,
               verticalalignment='top', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))