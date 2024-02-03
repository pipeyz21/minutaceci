import pandas as pd
import os
import re
import nltk
from dotenv import load_dotenv
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from tabla_base import TablaBase

def download_nltk_resources():
    nltk.download('stopwords')
    nltk.download('punkt')

class TextSimiliratyCalculator:
    def __init__(self, df):
        self.df = df
        self.stop_words = set(stopwords.words('spanish'))

    def preprocess_text(self, text):
        words = word_tokenize(text.lower())
        filtered_words = [word for word in words if word.isalnum() and word not in self.stop_words]
        return ' '.join(filtered_words)
    
    def calculate_similarity_matrix(self):
        self.df['Mensaje_Preprocesado'] = self.df['Mensaje'].apply(self.preprocess_text)

        vectorizer = TfidfVectorizer()
        tfidf_matrix = vectorizer.fit_transform(self.df['Mensaje_Preprocesado'])

        similarity_matrix = cosine_similarity(tfidf_matrix, tfidf_matrix)

        similarity_df = pd.DataFrame(similarity_matrix, index=self.df['id'], columns=self.df['id'])
        return similarity_df


class TablaOferta(TablaBase):
    def __init__(self):
        super().__init__()
        self.datos = {'Fecha': [], 'Hora': [], 'Mensaje': []}

    def _conversion_df(self, ruta_origen, ruta_destino):
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

        # Creamos el DataFrame
        df = pd.DataFrame(self.datos).reset_index().rename(columns={'index':'id'})

        # Guardamos el DataFrame como CSV
        # df.to_csv(ruta_destino, index=True)
        print('DataFrame guardado correctamente')

        return df
                    


if __name__ == '__main__':
    load_dotenv()
    download_nltk_resources()

    oferta = TablaOferta()
    oferta = oferta._conversion_df(os.getenv('RUTA_OFERTA_SIN_PROCESAR'), os.getenv('RUTA_OFERTA_DIARIA'))
    # print(oferta)

    similarity_calculator = TextSimiliratyCalculator(oferta)
    similarity_matrix = similarity_calculator.calculate_similarity_matrix()

    id1 = 1
    id2 = 2
    similarity_value = similarity_matrix.loc[id1, id2]

    print(f"Similitud entre mensaje {id1} y mensaje {id2}: {similarity_value}")

    # print(similiraty_matrix)

    
