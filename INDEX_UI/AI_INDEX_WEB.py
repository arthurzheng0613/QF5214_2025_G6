from flask import Flask, request, render_template, abort
import mysql.connector
import pandas as pd
import folium
import io
import base64
import matplotlib.pyplot as plt
import seaborn as sns

app = Flask(__name__)

def get_connection():
    return mysql.connector.connect(
        user='g6_qf5214_1',
        password='qf5214_1',
        host='rm-zf8608n9yz8lywnj1go.mysql.kualalumpur.rds.aliyuncs.com',
        port=3306,
        database='nasa_power'
    )

@app.route('/', methods=['GET', 'POST'])
def index():
    ai_value = None
    heatmap_img = None
    folium_map_html = None

    if request.method == 'POST':
        lat = float(request.form['latitude'])
        lon = float(request.form['longitude'])
        date = request.form['date']

        conn = get_connection()

        query1 = f"""
            SELECT AI_EXPERT_INDEX FROM agri_ai_index
            WHERE Latitude = {lat} AND Longitude = {lon} AND Timestamp = '{date}'
        """
        df_point = pd.read_sql(query1, conn)
        if not df_point.empty:
            ai_value = round(df_point.iloc[0]['AI_EXPERT_INDEX'], 3)

        query2 = f"""
            SELECT Latitude, Longitude, AI_EXPERT_INDEX FROM agri_ai_index
            WHERE Timestamp = '{date}'
        """
        df_heat = pd.read_sql(query2, conn)
        conn.close()

        if not df_heat.empty:
            pivot = df_heat.pivot(index='Latitude', columns='Longitude', values='AI_EXPERT_INDEX')
            fig, ax = plt.subplots(figsize=(8, 5))
            sns.heatmap(pivot.sort_index(ascending=False), cmap='coolwarm', ax=ax)
            ax.set_title(f'Heatmap on {date}')
            buf = io.BytesIO()
            plt.savefig(buf, format='png')
            buf.seek(0)
            heatmap_img = base64.b64encode(buf.getvalue()).decode('utf8')
            plt.close()

            m = folium.Map(location=[df_heat['Latitude'].mean(), df_heat['Longitude'].mean()], zoom_start=4)
            for _, row in df_heat.iterrows():
                folium.CircleMarker(
                    location=[row['Latitude'], row['Longitude']],
                    radius=3,
                    color='blue',
                    fill=True,
                    fill_opacity=0.6,
                    popup=f"{row['AI_EXPERT_INDEX']:.2f}"
                ).add_to(m)
            folium_map_html = m._repr_html_()

    return render_template("index.html", ai_value=ai_value,
                           heatmap_img=heatmap_img,
                           folium_map_html=folium_map_html)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
