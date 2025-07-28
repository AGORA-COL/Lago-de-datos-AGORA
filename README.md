# _Lago de datos ÁGORA_

El **Lago de Datos ÁGORA** es una infraestructura de datos creada en la **Pontificia Universidad Javeriana** como parte del proyecto [ÁGORA](https://agora-colombia.com/), financiado por los Ministerios de Ciencia, Tecnología e Innovación y de Salud y Protección Social de Colombia.

Reúne información en salud pública de toda Colombia entre los años **2009 y 2023**, con el propósito de integrar, caracterizar y analizar cuantitativamente los datos del sistema de salud colombiano antes, durante y después de la pandemia por COVID-19.

---

## Objetivos del Lago de Datos

1. Integrar fuentes masivas de información del sistema de salud en Colombia.
2. Caracterizar la atención en salud en distintos momentos de la pandemia.
3. Analizar mediante modelos descriptivos y predictivos el impacto de medidas farmacéuticas y no farmacéuticas.

---

## Fuentes de Información

El lago de datos integra microdatos anonimizados provenientes de múltiples fuentes oficiales como:

- **RIPS** (Registros Individuales de Prestación de Servicios de Salud)
- **Estadísticas Vitales** (Nacimientos y Defunciones)
- **SegCovid-19**
- **SIVIGILA**
- **Plan Nacional de Vacunación contra COVID-19**
- **Datos públicos**: ocupación hospitalaria, movilidad, demografía, entre otros.

Todos los datos cuentan con un pseudoidentificador no reversible para permitir su interoperabilidad de forma segura.

⚠️ **Importante:** Este repositorio no contiene los datos sensibles ni los microdatos del lago de datos ÁGORA. Aquí se encuentra únicamente la estructura técnica para el montaje del lago y los procesos de extracción, transformación y carga (ETL) aplicables a los datos crudos.

Si necesitas información específica de alguna base de datos o conjunto de variables, por favor contacta a los autores. La información contenida en el lago es sensible y está bajo custodia del proyecto, por lo que su acceso se gestiona de forma controlada y confidencial.

---

## Tabla 1. Volúmenes de información registrados

| Fuente                      | Período            | Registros (millones) |
|----------------------------|--------------------|----------------------|
| Defunciones                | 2014-01 a 2023-12  | 2,49                 |
| Nacimientos                | 2014-01 a 2023-12  | 5,90                 |
| RIPS                       | 2009-01 a 2022-12  | 4.262,86             |
| SegCovid-19                | 2020-03 a 2023-04  | 18,43                |
| SIVIGILA (otros eventos)   | 2009-01 a 2023-12  | 9,51                 |
| SIVIGILA-IRA (ficha 346)   | 2020-03 a 2023-12  | 6,24                 |
| Vacunación                 | 2021-01 a 2023-12  | 81,58                |
| **Total**                  |                    | **4387,1**           |

---

## Tabla 2. Coincidencia de identificadores entre fuentes (%)

|       | DEF   | NAC   | RIPS  | SEG   | SIV   | SIV-346 | VAC   |
|-------|-------|-------|-------|-------|-------|---------|-------|
| **DEF**     | 100.0 | 0.7   | 91.7  | 8.6   | 10.9  | 8.4     | 23.9  |
| **NAC**     | 0.4   | 100.0 | 97.1  | 11.9  | 20.5  | 12.7    | 72.8  |
| **RIPS**    | 3.2   | 6.4   | 100.0 | 7.5   | 9.0   | 8.0     | 48.7  |
| **SEG**     | 3.9   | 10.1  | 97.1  | 100.0 | 11.9  | 97.8    | 87.3  |
| **SIV**     | 3.9   | 13.5  | 91.1  | 9.2   | 100.0 | 9.4     | 54.0  |
| **SIV-346** | 3.6   | 10.1  | 96.6  | 90.7  | 11.3  | 100.0   | 87.2  |
| **VAC**     | 1.5   | 8.7   | 89.2  | 12.3  | 9.8   | 13.2    | 100.0 |

---

## Arquitectura del Lago de Datos

El lago se despliega sobre el **HPC-ZINE**, utilizando tecnología de Big Data:

- **HDFS**: almacenamiento distribuido.
- **Spark**: procesamiento distribuido de datos.
- **YARN**: gestión de recursos y programación de tareas.

### Zonas del lago:

- **Raw Data**: datos originales sin procesamiento.
- **Stage Data**: fuentes estandarizadas y con limpieza básica.
- **Analytic Data**: datos integrados y listos para análisis.

---

## Acceso y procesamiento

El acceso se realiza vía **Jupyter Notebooks** usando **R o Python**, sobre un clúster privado con acceso restringido vía VPN.

---

## Agrupación de enfermedades (CIE-10)

Para facilitar el análisis, se utilizan dos sistemas de agrupación de códigos:

1. Sistema de agrupación ÁGORA
Agrupa 12.654 códigos CIE-10 en 15 categorías relevantes para el contexto colombiano, como:

- Alteraciones visuales o auditivas (335 códigos).
- Condiciones asociadas a lesiones o agresiones (4.692 códigos).
- COVID-19 (5 códigos).
- Enfermedades cardiovasculares y metabólicas (639 códigos).
- Enfermedades de los sistemas digestivo o urinario (727 códigos).
- Enfermedades infecciosas (1.095 códigos).
- Enfermedades osteomusculares y degenerativas (528 códigos).
- Enfermedades respiratorias crónicas o de la piel o estructuras anexas (460 códigos).
- Factores relacionados con el contacto con los servicios de salud (592 códigos).
- Salud oral (105 códigos).
- Signos y síntomas mal definidos (311 códigos).
- Trastornos materno-perinatales, congénitos o nutricionales (1.501 códigos).
- Trastornos neurológicos o mentales (726 códigos).
- Tumores, enfermedades hematopoyéticas y del sistema inmune (921 códigos).
- No válido para análisis (17 códigos).

2. Sistema basado en Charlson
Agrupación centrada en comorbilidades crónicas relevantes. Incluye:

- Cualquier tipo de malignidad, incluyendo linfoma y leucemia, excepto neoplasias malignas de piel (432 códigos)
- Demencia (23 códigos)
- Diabetes con complicaciones crónicas (25 códigos)
- Diabetes sin complicaciones crónicas (25 códigos)
- Enfermedad cerebrovascular (82 códigos)
- Enfermedad pulmonar crónica (52 códigos)
- Enfermedad hepática leve (33 códigos)
- Enfermedad hepática moderada o severa (11 códigos)
- Enfermedad vascular periférica (25 códigos)
- Enfermedad renal (31 códigos)
- Enfermedades reumáticas (26 códigos) 
- Hemiplejía o paraplejía (19 códigos)
- Insuficiencia cardíaca congestiva (18 códigos)
- Infarto de miocardio (11 códigos)
- SIDA/VIH (22 códigos)
- Tumor sólido metastásico (9 códigos)
- Úlcera péptica (36 códigos)


**Nota:** Estas mismas agrupaciones han sido implementadas en el software R mediante el paquete [epiAgora](https://github.com/AGORA-COL/epiAgora), el cual facilita su aplicación en conjuntos de datos estructurados bajo el lago de datos ÁGORA u otras bases con códigos CIE-10.

---

## Colaboraciones

Este repositorio y desarrollo han sido posibles gracias al trabajo conjunto del equipo ÁGORA y la alianza **CAOBA**.

---

## Contribuciones
Las contribuciones son bienvenidas. Puedes:

- Hacer un fork del repositorio
- Crear una rama con tus cambios
- Enviar un Pull Request

---

## Autores
- Andrés Moreno.
- Jennifer Murillo-Alvarado.
- Daniel Santiago Bonilla Betancourth.
- Johan Manuel Calderón Rodríguez.
- Juan Guillermo Torres.
- Jaime Pavlich-Mariscal.
- Zulma M. Cucunubá.
- Alianza Caoba

---

## Financiación
Esta investigación fue financiada por el Ministerio de Ciencia, Tecnología e Innovación de Colombia, proyecto ÁGORA: “Alianza para la Generación de Evidencia sobre COVID-19, su Respuesta y Lecciones Aprendidas para la Postpandemia y Futuras Epidemias” (Contrato N° 637-2022).

---

## Cómo citar este recurso
Si utilizas esta rutina o sus sistemas de agrupación en tus análisis o publicaciones, por favor citarlo de la siguiente manera:

Moreno, A., Murillo, J., Bonilla, D., Calderón Rodríguez, J. M., Torres, J. G., Pavlich-Mariscal, J. A., Cucunubá, Z. M., & Alianza Caoba. (2025). ÁGORA: Ciencia y decisiones en salud pública. Proyecto ÁGORA Colombia. 

También puedes exportar esta cita en formatos como BibTeX, RIS, APA y más desde el botón “Cite this repository” en la parte superior derecha de este repositorio (disponible si has agregado el archivo CITATION.cff).

---

## Contacto
Si tienes preguntas, sugerencias o comentarios, por favor crea un Issue en este repositorio.

