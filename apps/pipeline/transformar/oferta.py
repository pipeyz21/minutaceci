import pandas as pd
import os
import re
import nltk
from dotenv import load_dotenv
from sklearn.cluster import KMeans
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from tabla_base import TablaBase

def download_nltk_resources():
    nltk.download('stopwords')
    nltk.download('punkt')

class TextSimiliratyCalculator:
    def __init__(self):
        self.stop_words = set(stopwords.words('spanish'))

    def preprocess_text(self, text):
        words = word_tokenize(text.lower())
        filtered_words = [word for word in words if word.isalnum() and word not in self.stop_words]
        return ' '.join(filtered_words)
    
    def calculate_similarity_matrix(self, df):
        df['Mensaje_Preprocesado'] = df['Mensaje'].apply(self.preprocess_text)

        vectorizer = TfidfVectorizer()
        tfidf_matrix = vectorizer.fit_transform(df['Mensaje_Preprocesado'])

        similarity_matrix = cosine_similarity(tfidf_matrix, tfidf_matrix)

        similarity_df = pd.DataFrame(similarity_matrix, index=df['id'], columns=df['id'])
        return similarity_df


class TablaOferta(TablaBase, TextSimiliratyCalculator):
    def __init__(self):
        super().__init__()
        self.stop_words = set(stopwords.words('spanish'))
        self.datos = {'Fecha': [], 'Hora': [], 'Mensaje': []}

    def _conversion_df(self, ruta_origen):
        with open(ruta_origen, 'r', encoding='utf-8') as archivo:
            # Variables para almacenar información temporal
            fecha = ''
            hora = ''
            mensaje = ''

            for linea in archivo:
                # Utilizamos expresiones regulares para buscar el remitente
                match = re.search(r'- (Cecilia Zuñiga): (.+)', linea)

                # Si encontramos una coincidencia
                if match:
                    # Si hay información almacenada previamente, la agregamos al DataFrame
                    if fecha and hora and mensaje:
                        self.datos['Fecha'].append(fecha)
                        self.datos['Hora'].append(hora)
                        self.datos['Mensaje'].append(mensaje)

                # Extremos la fecha, hora y mensaje
                    partes = linea.strip().split('-')
                    fecha, hora = partes[0].split(',')
                    mensaje = partes[1].split(':', 1)[1].strip()
                
                else:
                    # Si no hay coincidencia, añadimos la línea al mensaje acutal
                    mensaje += linea.strip() + " "

        # Agregamos el último conjunto de datos al DataFrame
        self.datos['Fecha'].append(fecha)
        self.datos['Hora'].append(hora)
        self.datos['Mensaje'].append(mensaje)

        # Creamos y filtramos el DataFrame 
        df = pd.DataFrame(self.datos).reset_index().rename(columns={'index':'id'})
        # df = df[(df['Mensaje'] != 'Eliminaste este mensaje.') & (df['Mensaje']!='<Multimedia omitido>')]

        # Guardamos el DataFrame como CSV
        return df
    
    def clustering(self, ruta_origen, ruta_destino):
        df = self._conversion_df(ruta_origen)
        matriz_similitud = self.calculate_similarity_matrix(df)

        kmeans = KMeans(7, random_state=42)
        clusters = kmeans.fit_predict(matriz_similitud)

        df['cluster'] = clusters

        df.to_csv(ruta_destino, index=True)
        print('DataFrame guardado correctamente')

        return df
                    


if __name__ == '__main__':
    load_dotenv()
    download_nltk_resources()

    oferta = TablaOferta()
    oferta = oferta.clustering(os.getenv('RUTA_OFERTA_SIN_PROCESAR'), os.getenv('RUTA_OFERTA_DIARIA'))
    
    


    



    
