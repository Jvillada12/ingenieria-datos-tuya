# Arquitectura Dataset de Números de Teléfono

**Desarrollado por:** Johnnatan Villada Flórez

## Resumen 

Diseño e implementación de un proceso automatizado y controlado mediante prácticas de CI/CD para la creación, validación, despliegue y mantenimiento de un dataset confiable de números de teléfono de clientes.

## Objetivos

- **Calidad**: Garantizar la integridad y precisión de los datos telefónicos
- **Automatización**: Minimizar intervención manual mediante CI/CD
- **Trazabilidad**: Registro completo del linaje de datos
- **Escalabilidad**: Arquitectura que soporte crecimiento del negocio
- **Compliance**: Cumplimiento de GDPR, LOPD y regulaciones locales

## Arquitectura de Solución

```mermaid
graph TB
    subgraph "Fuentes de Datos"
        A1[CRM Salesforce]
        A2[Call Center]
        A3[Web Forms]
        A4[Mobile App]
        A5[Archivo Manual]
    end

    subgraph "Ingesta y Validación"
        B1[API Gateway]
        B2[Event Streaming]
        B3[Validation Engine]
        B4[Data Quality Rules]
    end

    subgraph "Procesamiento"
        C1[Normalization Service]
        C2[Deduplication Engine]
        C3[Enrichment Service]
        C4[ML Validation Model]
    end

    subgraph "Storage Layer"
        D1[Raw Data Lake]
        D2[Validated Dataset]
        D3[Master Phone Registry]
        D4[Audit Trail DB]
    end

    subgraph "CI/CD Pipeline"
        E1[GitHub Actions]
        E2[Data Tests]
        E3[Quality Gates]
        E4[Deployment]
    end

    subgraph "Consumption"
        F1[REST API]
        F2[GraphQL]
        F3[Streaming]
        F4[Batch Export]
    end

    A1 --> B1
    A2 --> B2
    A3 --> B1
    A4 --> B2
    A5 --> B1

    B1 --> B3
    B2 --> B3
    B3 --> B4
    B4 --> C1

    C1 --> C2
    C2 --> C3
    C3 --> C4
    C4 --> D2

    D1 --> D2
    D2 --> D3
    D3 --> D4

    D2 --> E1
    E1 --> E2
    E2 --> E3
    E3 --> E4

    D3 --> F1
    D3 --> F2
    D3 --> F3
    D3 --> F4

    style A1 fill:#e1f5fe
    style A2 fill:#e1f5fe
    style A3 fill:#e1f5fe
    style A4 fill:#e1f5fe
    style A5 fill:#e1f5fe
    style C4 fill:#fff3e0
    style D3 fill:#e8f5e8
    style E1 fill:#fce4ec
```

## Componentes Técnicos

### 1. Capa de Ingesta

#### API Gateway
```yaml
Tecnología: Kong/AWS API Gateway
Funciones:
  - Rate limiting por cliente
  - Autenticación OAuth 2.0
  - Transformación de formatos
  - Logging y métricas
  - Versionado de APIs
```

#### Event Streaming
```yaml
Tecnología: Apache Kafka/AWS Kinesis
Configuración:
  - Topic: customer-phone-updates
  - Partitions: 12 (por región)
  - Replication: 3
  - Retention: 30 días
```

### 2. Motor de Validación

#### Reglas de Calidad de Datos
```python
class PhoneValidationRules:
    """Reglas de validación para números telefónicos"""
    
    RULES = {
        'format_validation': {
            'colombia_mobile': r'^(\+57|0057|57)?[3][0-9]{9}$',
            'colombia_fixed': r'^(\+57|0057|57)?[1-8][0-9]{7}$',
            'international': r'^\+[1-9]\d{1,14}$'
        },
        'business_rules': {
            'max_phones_per_customer': 5,
            'min_phone_length': 10,
            'max_phone_length': 15,
            'blacklist_check': True,
            'carrier_validation': True
        },
        'data_quality': {
            'completeness': 0.95,  # 95% de registros completos
            'uniqueness': 0.99,    # 99% únicos por cliente
            'accuracy': 0.98,      # 98% formato válido
            'timeliness': '24h'    # Actualización máxima 24h
        }
    }
```

### 3. Servicios de Procesamiento

#### Normalización
```python
class PhoneNormalizationService:
    """Servicio de normalización de números telefónicos"""
    
    def normalize_phone(self, raw_phone: str, country_code: str = 'CO') -> dict:
        """
        Normaliza número telefónico a formato estándar E.164
        
        Returns:
            {
                'original': '+57 300 123 4567',
                'normalized': '+573001234567',
                'country_code': 'CO',
                'carrier': 'Claro',
                'type': 'mobile',
                'is_valid': True,
                'confidence_score': 0.98
            }
        """
```

#### Deduplicación
```python
class DeduplicationEngine:
    """Motor de deduplicación con ML"""
    
    def deduplicate(self, phone_records: List[PhoneRecord]) -> List[PhoneRecord]:
        """
        Algoritmo de deduplicación usando:
        - Fuzzy matching (Levenshtein distance)
        - Clustering por similitud
        - ML model para scoring de duplicados
        """
```

### 4. Modelo de Datos

#### Esquema del Dataset
```sql
-- Tabla principal de teléfonos
CREATE TABLE customer_phones (
    phone_id SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    phone_number VARCHAR(20) NOT NULL,
    phone_type VARCHAR(20) NOT NULL, -- mobile, fixed, fax
    country_code CHAR(2) NOT NULL,
    carrier VARCHAR(50),
    is_primary BOOLEAN DEFAULT FALSE,
    is_verified BOOLEAN DEFAULT FALSE,
    verification_method VARCHAR(50),
    confidence_score DECIMAL(3,2),
    source_system VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(customer_id, phone_number)
);

-- Auditoría y linaje
CREATE TABLE phone_audit_trail (
    audit_id SERIAL PRIMARY KEY,
    phone_id INTEGER REFERENCES customer_phones(phone_id),
    operation VARCHAR(20), -- INSERT, UPDATE, DELETE
    old_values JSONB,
    new_values JSONB,
    changed_by VARCHAR(100),
    change_reason VARCHAR(500),
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Métricas de calidad
CREATE TABLE data_quality_metrics (
    metric_id SERIAL PRIMARY KEY,
    execution_date DATE,
    dataset_version VARCHAR(20),
    total_records INTEGER,
    valid_records INTEGER,
    invalid_records INTEGER,
    duplicate_records INTEGER,
    completeness_score DECIMAL(5,2),
    accuracy_score DECIMAL(5,2),
    uniqueness_score DECIMAL(5,2),
    quality_grade CHAR(1) -- A, B, C, D, F
);
```

## Pipeline CI/CD

### Configuración GitHub Actions
```yaml
name: Phone Dataset CI/CD Pipeline

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main]
  schedule:
    - cron: '0 2 * * *'  # Ejecución diaria a las 2 AM

jobs:
  data-validation:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.9'
          
      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          pip install great-expectations pandas-profiling
          
      - name: Data Quality Tests
        run: |
          python tests/test_data_quality.py
          python tests/test_phone_validation.py
          
      - name: Generate Data Profile
        run: |
          python scripts/generate_data_profile.py
          
      - name: Upload Test Results
        uses: actions/upload-artifact@v3
        with:
          name: test-results
          path: test-results/

  security-scan:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: PII Detection
        run: |
          python scripts/scan_pii_data.py
          
      - name: Vulnerability Scan
        run: |
          bandit -r src/
          safety check

  deployment:
    needs: [data-validation, security-scan]
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/main'
    steps:
      - name: Deploy to Production
        run: |
          python scripts/deploy_dataset.py --env=prod
          
      - name: Update Documentation
        run: |
          python scripts/update_data_catalog.py
```

### Tests de Calidad de Datos
```python
import great_expectations as ge
from great_expectations.dataset import PandasDataset

class PhoneDatasetTests:
    """Test suite para validación del dataset de teléfonos"""
    
    def test_phone_format_validity(self, df: PandasDataset):
        """Valida que todos los teléfonos tengan formato válido"""
        df.expect_column_values_to_match_regex(
            'phone_number',
            r'^\+[1-9]\d{1,14}$'
        )
    
    def test_customer_phone_uniqueness(self, df: PandasDataset):
        """Valida unicidad por cliente"""
        df.expect_compound_columns_to_be_unique(
            ['customer_id', 'phone_number']
        )
    
    def test_completeness_threshold(self, df: PandasDataset):
        """Valida umbral de completitud"""
        df.expect_column_values_to_not_be_null(
            'phone_number',
            mostly=0.95
        )
    
    def test_data_freshness(self, df: PandasDataset):
        """Valida frescura de los datos"""
        df.expect_column_max_to_be_between(
            'updated_at',
            min_value=datetime.now() - timedelta(hours=24),
            max_value=datetime.now()
        )
```

## Monitoreo y Observabilidad

### Métricas Clave
```python
class PhoneDatasetMetrics:
    """Métricas de monitoreo del dataset"""
    
    KPIs = {
        'data_quality': {
            'completeness': 0.95,      # >= 95%
            'accuracy': 0.98,          # >= 98%
            'uniqueness': 0.99,        # >= 99%
            'timeliness': 24,          # <= 24 horas
            'validity': 0.97           # >= 97%
        },
        'operational': {
            'ingestion_latency': 300,   # <= 5 minutos
            'processing_time': 1800,   # <= 30 minutos
            'api_response_time': 200,  # <= 200ms
            'uptime': 0.999            # >= 99.9%
        },
        'business': {
            'phones_per_customer': 2.3,    # Promedio
            'verification_rate': 0.85,     # >= 85%
            'update_frequency': 30,        # Días promedio
            'coverage_rate': 0.92          # >= 92%
        }
    }
```

### Dashboard de Monitoreo
```python
# Configuración Grafana/Prometheus
monitoring_config = {
    'alerts': {
        'data_quality_drop': {
            'condition': 'accuracy < 0.95',
            'severity': 'critical',
            'notification': ['email', 'slack']
        },
        'ingestion_failure': {
            'condition': 'ingestion_errors > 10',
            'severity': 'warning',
            'notification': ['slack']
        },
        'api_latency_high': {
            'condition': 'avg_response_time > 500ms',
            'severity': 'warning',
            'notification': ['pagerduty']
        }
    }
}
```

## Seguridad y Compliance

### Medidas de Seguridad
- **Encriptación**: AES-256 en reposo, TLS 1.3 en tránsito
- **Tokenización**: PII tokenizado para ambientes no-productivos
- **Acceso**: RBAC con principio de menor privilegio
- **Auditoría**: Log completo de todos los accesos y cambios

### Compliance GDPR/LOPD
```python
class PrivacyControls:
    """Controles de privacidad y compliance"""
    
    def anonymize_phone(self, phone: str) -> str:
        """Anonimiza número para reportes"""
        return f"{phone[:5]}****{phone[-2:]}"
    
    def right_to_erasure(self, customer_id: str):
        """Implementa derecho al olvido"""
        # Soft delete con período de retención
        
    def data_portability(self, customer_id: str) -> dict:
        """Exporta datos del cliente en formato estándar"""
        # Retorna JSON con todos los datos del cliente
```

## Despliegue y Operación

### Ambientes
1. **Development**: Datos sintéticos, testing continuo
2. **Staging**: Subset de datos reales, validación pre-producción
3. **Production**: Dataset completo, alta disponibilidad

### Procedimientos Operativos
- **Backup**: Diario incremental, semanal completo
- **Disaster Recovery**: RTO < 4 horas, RPO < 1 hora
- **Capacity Planning**: Monitoreo automático de recursos
- **Performance Tuning**: Optimización continua de consultas

## Roadmap de Evolución

### Fase 1 (MVP - 3 meses)
- Pipeline básico de ingesta
- Validaciones fundamentales
- API REST básica
- Dashboard de monitoreo

### Fase 2 (Enriquecimiento - 6 meses)
- ML para validación inteligente
- Enriquecimiento con datos externos
- API GraphQL
- Real-time streaming

### Fase 3 (Optimización - 12 meses)
- Auto-healing capabilities
- Predictive quality scoring
- Advanced analytics
- Multi-cloud deployment

## Implementación Detallada

### Configuración de Infraestructura

#### Docker Compose para Desarrollo
```yaml
version: '3.8'

services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: phone_dataset
      POSTGRES_USER: app_user
      POSTGRES_PASSWORD: secure_password
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./sql/init.sql:/docker-entrypoint-initdb.d/init.sql
    ports:
      - "5432:5432"

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data

  kafka:
    image: confluentinc/cp-kafka:latest
    environment:
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"

  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000

  phone-api:
    build: .
    ports:
      - "8080:8080"
    environment:
      - DATABASE_URL=postgresql://app_user:secure_password@postgres:5432/phone_dataset
      - REDIS_URL=redis://redis:6379
      - KAFKA_BOOTSTRAP_SERVERS=kafka:9092
    depends_on:
      - postgres
      - redis
      - kafka

volumes:
  postgres_data:
  redis_data:
```

### Servicios de Procesamiento

#### Servicio de Enriquecimiento
```python
import asyncio
import aiohttp
from typing import Dict, Optional

class PhoneEnrichmentService:
    """Servicio de enriquecimiento de números telefónicos"""
    
    def __init__(self):
        self.carrier_api_url = "https://api.carrier-lookup.com/v1"
        self.geolocation_api_url = "https://api.geolocation.com/v1"
        
    async def enrich_phone(self, phone_number: str) -> Dict:
        """Enriquece un número telefónico con información externa"""
        
        tasks = [
            self._get_carrier_info(phone_number),
            self._get_geolocation_info(phone_number),
            self._get_number_type(phone_number)
        ]
        
        carrier_info, geo_info, number_type = await asyncio.gather(*tasks)
        
        return {
            'phone_number': phone_number,
            'carrier': carrier_info,
            'geolocation': geo_info,
            'type': number_type,
            'enriched_at': datetime.utcnow().isoformat()
        }
    
    async def _get_carrier_info(self, phone_number: str) -> Optional[Dict]:
        """Obtiene información del operador"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{self.carrier_api_url}/lookup/{phone_number}"
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        return {
                            'name': data.get('carrier_name'),
                            'type': data.get('carrier_type'),
                            'country': data.get('country')
                        }
        except Exception as e:
            logger.error(f"Error getting carrier info: {e}")
            return None
    
    async def _get_geolocation_info(self, phone_number: str) -> Optional[Dict]:
        """Obtiene información de geolocalización"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{self.geolocation_api_url}/phone/{phone_number}"
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        return {
                            'country': data.get('country'),
                            'region': data.get('region'),
                            'city': data.get('city'),
                            'timezone': data.get('timezone')
                        }
        except Exception as e:
            logger.error(f"Error getting geolocation info: {e}")
            return None
    
    async def _get_number_type(self, phone_number: str) -> str:
        """Determina el tipo de número (mobile, fixed, voip, etc.)"""
        # Implementar lógica de detección de tipo
        if phone_number.startswith('+573'):
            return 'mobile'
        elif phone_number.startswith('+571'):
            return 'fixed'
        else:
            return 'unknown'
```

#### Motor de Deduplicación Avanzado
```python
import numpy as np
from sklearn.cluster import DBSCAN
from sklearn.feature_extraction.text import TfidfVectorizer
from difflib import SequenceMatcher

class AdvancedDeduplicationEngine:
    """Motor avanzado de deduplicación usando ML"""
    
    def __init__(self):
        self.vectorizer = TfidfVectorizer(analyzer='char', ngram_range=(2, 4))
        self.clustering_model = DBSCAN(eps=0.3, min_samples=2)
        
    def find_duplicates(self, phone_records: List[Dict]) -> List[List[int]]:
        """
        Encuentra grupos de registros duplicados
        
        Returns:
            Lista de listas, cada sublista contiene índices de registros duplicados
        """
        # Extraer características para clustering
        features = self._extract_features(phone_records)
        
        # Aplicar clustering
        clusters = self.clustering_model.fit_predict(features)
        
        # Agrupar índices por cluster
        duplicate_groups = {}
        for idx, cluster_id in enumerate(clusters):
            if cluster_id != -1:  # -1 indica outliers (no duplicados)
                if cluster_id not in duplicate_groups:
                    duplicate_groups[cluster_id] = []
                duplicate_groups[cluster_id].append(idx)
        
        # Filtrar grupos con más de un elemento
        return [group for group in duplicate_groups.values() if len(group) > 1]
    
    def _extract_features(self, phone_records: List[Dict]) -> np.ndarray:
        """Extrae características para el clustering"""
        # Combinar información relevante para duplicación
        combined_strings = []
        
        for record in phone_records:
            combined = f"{record.get('phone_number', '')}{record.get('customer_id', '')}"
            combined_strings.append(combined)
        
        # Vectorizar usando TF-IDF
        return self.vectorizer.fit_transform(combined_strings).toarray()
    
    def calculate_similarity(self, phone1: str, phone2: str) -> float:
        """Calcula similitud entre dos números telefónicos"""
        # Normalizar números para comparación
        clean_phone1 = self._clean_phone(phone1)
        clean_phone2 = self._clean_phone(phone2)
        
        # Usar SequenceMatcher para similitud
        return SequenceMatcher(None, clean_phone1, clean_phone2).ratio()
    
    def _clean_phone(self, phone: str) -> str:
        """Limpia número telefónico para comparación"""
        # Remover espacios, guiones, paréntesis, etc.
        import re
        return re.sub(r'[^\d+]', '', phone)
    
    def resolve_duplicates(self, duplicate_group: List[Dict]) -> Dict:
        """
        Resuelve un grupo de duplicados seleccionando el mejor registro
        
        Criterios de selección:
        1. Más reciente (updated_at)
        2. Mayor confidence_score
        3. Verificado vs no verificado
        4. Fuente más confiable
        """
        if not duplicate_group:
            return None
        
        # Ordenar por criterios de calidad
        sorted_records = sorted(duplicate_group, key=lambda x: (
            x.get('is_verified', False),
            x.get('confidence_score', 0),
            x.get('updated_at', ''),
            self._get_source_priority(x.get('source_system', ''))
        ), reverse=True)
        
        return sorted_records[0]
    
    def _get_source_priority(self, source_system: str) -> int:
        """Retorna prioridad de la fuente (mayor es mejor)"""
        priorities = {
            'crm_salesforce': 5,
            'call_center': 4,
            'web_forms': 3,
            'mobile_app': 2,
            'manual_upload': 1
        }
        return priorities.get(source_system.lower(), 0)
```

### APIs de Consumo

#### API REST con FastAPI
```python
from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from typing import List, Optional
import asyncio

app = FastAPI(
    title="Phone Dataset API",
    description="API para acceso al dataset de números telefónicos",
    version="1.0.0"
)

security = HTTPBearer()

class PhoneRecord(BaseModel):
    phone_id: Optional[int]
    customer_id: str
    phone_number: str
    phone_type: str
    country_code: str
    carrier: Optional[str]
    is_primary: bool = False
    is_verified: bool = False
    confidence_score: Optional[float]

class PhoneQuery(BaseModel):
    customer_ids: Optional[List[str]] = None
    phone_types: Optional[List[str]] = None
    countries: Optional[List[str]] = None
    verified_only: bool = False
    limit: int = 100
    offset: int = 0

@app.get("/phones/{customer_id}", response_model=List[PhoneRecord])
async def get_customer_phones(
    customer_id: str,
    verified_only: bool = Query(False),
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """Obtiene todos los teléfonos de un cliente específico"""
    # Validar token de autorización
    if not validate_token(credentials.credentials):
        raise HTTPException(status_code=401, detail="Invalid token")
    
    # Consultar base de datos
    phones = await phone_service.get_phones_by_customer(
        customer_id, 
        verified_only=verified_only
    )
    
    if not phones:
        raise HTTPException(status_code=404, detail="Customer not found")
    
    return phones

@app.post("/phones/search", response_model=List[PhoneRecord])
async def search_phones(
    query: PhoneQuery,
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """Búsqueda avanzada de números telefónicos"""
    if not validate_token(credentials.credentials):
        raise HTTPException(status_code=401, detail="Invalid token")
    
    result = await phone_service.search_phones(query)
    return result

@app.post("/phones/validate")
async def validate_phone(
    phone_number: str,
    country_code: str = "CO",
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """Valida un número telefónico"""
    if not validate_token(credentials.credentials):
        raise HTTPException(status_code=401, detail="Invalid token")
    
    validation_result = await phone_validation_service.validate(
        phone_number, 
        country_code
    )
    
    return validation_result

@app.get("/phones/health")
async def health_check():
    """Health check del servicio"""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "version": "1.0.0"
    }

def validate_token(token: str) -> bool:
    """Valida token de autorización"""
    # Implementar validación JWT o similar
    return True  # Placeholder
```

#### API GraphQL
```python
import strawberry
from typing import List, Optional

@strawberry.type
class Phone:
    phone_id: int
    customer_id: str
    phone_number: str
    phone_type: str
    country_code: str
    carrier: Optional[str]
    is_primary: bool
    is_verified: bool
    confidence_score: Optional[float]

@strawberry.type
class Customer:
    customer_id: str
    phones: List[Phone]

@strawberry.type
class Query:
    @strawberry.field
    def customer(self, customer_id: str) -> Optional[Customer]:
        """Obtiene un cliente con sus teléfonos"""
        return get_customer_with_phones(customer_id)
    
    @strawberry.field
    def phones(
        self, 
        customer_ids: Optional[List[str]] = None,
        phone_types: Optional[List[str]] = None,
        verified_only: bool = False,
        limit: int = 100
    ) -> List[Phone]:
        """Búsqueda de teléfonos con filtros"""
        return search_phones_graphql(
            customer_ids=customer_ids,
            phone_types=phone_types,
            verified_only=verified_only,
            limit=limit
        )

@strawberry.type
class Mutation:
    @strawberry.mutation
    def add_phone(self, phone_input: PhoneInput) -> Phone:
        """Añade un nuevo número telefónico"""
        return add_phone_to_dataset(phone_input)
    
    @strawberry.mutation
    def verify_phone(self, phone_id: int) -> Phone:
        """Marca un teléfono como verificado"""
        return mark_phone_verified(phone_id)

schema = strawberry.Schema(query=Query, mutation=Mutation)
```

### Monitoreo Avanzado

#### Métricas Personalizadas con Prometheus
```python
from prometheus_client import Counter, Histogram, Gauge, start_http_server
import time

# Métricas de aplicación
phone_validations_total = Counter(
    'phone_validations_total',
    'Total number of phone validations',
    ['status', 'country']
)

phone_validation_duration = Histogram(
    'phone_validation_duration_seconds',
    'Time spent validating phones'
)

active_phone_records = Gauge(
    'active_phone_records_total',
    'Total number of active phone records'
)

data_quality_score = Gauge(
    'data_quality_score',
    'Current data quality score',
    ['dimension']
)

class MetricsCollector:
    """Recolector de métricas personalizado"""
    
    def __init__(self):
        self.start_metrics_server()
    
    def start_metrics_server(self):
        """Inicia servidor de métricas Prometheus"""
        start_http_server(8000)
    
    def record_validation(self, status: str, country: str, duration: float):
        """Registra una validación de teléfono"""
        phone_validations_total.labels(status=status, country=country).inc()
        phone_validation_duration.observe(duration)
    
    def update_active_records(self, count: int):
        """Actualiza contador de registros activos"""
        active_phone_records.set(count)
    
    def update_quality_score(self, dimension: str, score: float):
        """Actualiza puntuación de calidad por dimensión"""
        data_quality_score.labels(dimension=dimension).set(score)

# Decorador para medir tiempo de ejecución
def measure_time(metric_name: str):
    def decorator(func):
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            duration = time.time() - start_time
            
            if metric_name == 'validation':
                phone_validation_duration.observe(duration)
            
            return result
        return wrapper
    return decorator
```

#### Dashboard Grafana
```json
{
  "dashboard": {
    "title": "Phone Dataset Monitoring",
    "panels": [
      {
        "title": "Data Quality Score",
        "type": "stat",
        "targets": [
          {
            "expr": "avg(data_quality_score)",
            "legendFormat": "Overall Quality"
          }
        ]
      },
      {
        "title": "Phone Validations Rate",
        "type": "graph",
        "targets": [
          {
            "expr": "rate(phone_validations_total[5m])",
            "legendFormat": "Validations/sec"
          }
        ]
      },
      {
        "title": "Active Phone Records",
        "type": "stat",
        "targets": [
          {
            "expr": "active_phone_records_total",
            "legendFormat": "Total Records"
          }
        ]
      },
      {
        "title": "Validation Latency",
        "type": "graph",
        "targets": [
          {
            "expr": "histogram_quantile(0.95, phone_validation_duration_seconds_bucket)",
            "legendFormat": "95th percentile"
          },
          {
            "expr": "histogram_quantile(0.50, phone_validation_duration_seconds_bucket)",
            "legendFormat": "50th percentile"
          }
        ]
      }
    ]
  }
}
```

### Sistema de Alertas Avanzado

#### Configuración de Alertmanager
```yaml
# alertmanager.yml
global:
  smtp_smarthost: 'localhost:587'
  smtp_from: 'alerts@company.com'

route:
  group_by: ['alertname']
  group_wait: 10s
  group_interval: 10s
  repeat_interval: 1h
  receiver: 'web.hook'
  routes:
  - match:
      severity: critical
    receiver: 'critical-alerts'
  - match:
      severity: warning
    receiver: 'warning-alerts'

receivers:
- name: 'web.hook'
  webhook_configs:
  - url: 'http://webhook-service:5000/alert'

- name: 'critical-alerts'
  email_configs:
  - to: 'engineering-team@company.com'
    subject: 'CRITICAL: Phone Dataset Alert'
    body: |
      Alert: {{ .GroupLabels.alertname }}
      Summary: {{ .CommonAnnotations.summary }}
      Description: {{ .CommonAnnotations.description }}
  slack_configs:
  - api_url: 'https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX'
    channel: '#data-engineering-alerts'
    title: 'Critical Phone Dataset Alert'

- name: 'warning-alerts'
  slack_configs:
  - api_url: 'https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX'
    channel: '#data-engineering'
    title: 'Phone Dataset Warning'
```

#### Reglas de Alertas Prometheus
```yaml
# phone_dataset_alerts.yml
groups:
- name: phone_dataset
  rules:
  - alert: DataQualityDropCritical
    expr: avg(data_quality_score) < 0.70
    for: 5m
    labels:
      severity: critical
    annotations:
      summary: "Data quality score has dropped below critical threshold"
      description: "Data quality score is {{ $value }}, below the critical threshold of 0.70"

  - alert: DataQualityDropWarning
    expr: avg(data_quality_score) < 0.85
    for: 10m
    labels:
      severity: warning
    annotations:
      summary: "Data quality score has dropped below warning threshold"
      description: "Data quality score is {{ $value }}, below the warning threshold of 0.85"

  - alert: HighValidationLatency
    expr: histogram_quantile(0.95, phone_validation_duration_seconds_bucket) > 1.0
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "Phone validation latency is high"
      description: "95th percentile latency is {{ $value }}s, above threshold of 1.0s"

  - alert: ValidationFailureRate
    expr: rate(phone_validations_total{status="failed"}[5m]) / rate(phone_validations_total[5m]) > 0.05
    for: 5m
    labels:
      severity: critical
    annotations:
      summary: "High phone validation failure rate"
      description: "Validation failure rate is {{ $value }}, above threshold of 5%"

  - alert: LowDataIngestionRate
    expr: rate(active_phone_records_total[10m]) < 10
    for: 10m
    labels:
      severity: warning
    annotations:
      summary: "Low data ingestion rate detected"
      description: "Data ingestion rate is {{ $value }} records/sec, below expected threshold"
```

### Herramientas de Gestión de Datos

#### CLI Tool para Administración
```python
import click
import asyncio
from datetime import datetime, timedelta

@click.group()
def cli():
    """Phone Dataset Management CLI"""
    pass

@cli.command()
@click.option('--customer-id', required=True, help='Customer ID to process')
@click.option('--dry-run', is_flag=True, help='Show what would be done without executing')
def deduplicate_customer(customer_id: str, dry_run: bool):
    """Deduplicate phones for a specific customer"""
    click.echo(f"Processing deduplication for customer: {customer_id}")
    
    if dry_run:
        click.echo("DRY RUN MODE - No changes will be made")
    
    # Implementar lógica de deduplicación
    result = asyncio.run(deduplicate_customer_phones(customer_id, dry_run))
    
    click.echo(f"Found {result['duplicates_found']} duplicate groups")
    if not dry_run:
        click.echo(f"Merged {result['records_merged']} records")

@cli.command()
@click.option('--start-date', type=click.DateTime(), help='Start date for quality check')
@click.option('--end-date', type=click.DateTime(), help='End date for quality check')
def quality_report(start_date: datetime, end_date: datetime):
    """Generate data quality report"""
    if not start_date:
        start_date = datetime.now() - timedelta(days=7)
    if not end_date:
        end_date = datetime.now()
    
    click.echo(f"Generating quality report from {start_date} to {end_date}")
    
    report = asyncio.run(generate_quality_report(start_date, end_date))
    
    click.echo("\nData Quality Report")
    click.echo("=" * 50)
    click.echo(f"Total Records: {report['total_records']:,}")
    click.echo(f"Valid Records: {report['valid_records']:,}")
    click.echo(f"Completeness: {report['completeness']:.2%}")
    click.echo(f"Accuracy: {report['accuracy']:.2%}")
    click.echo(f"Uniqueness: {report['uniqueness']:.2%}")

@cli.command()
@click.option('--source', required=True, help='Data source identifier')
@click.option('--file-path', required=True, help='Path to data file')
@click.option('--format', type=click.Choice(['csv', 'json', 'xlsx']), default='csv')
def import_data(source: str, file_path: str, format: str):
    """Import phone data from file"""
    click.echo(f"Importing data from {file_path} (format: {format})")
    
    result = asyncio.run(import_phone_data(source, file_path, format))
    
    click.echo(f"Imported {result['imported']} records")
    click.echo(f"Skipped {result['skipped']} invalid records")
    if result['errors']:
        click.echo(f"Errors: {len(result['errors'])}")
        for error in result['errors'][:5]:  # Show first 5 errors
            click.echo(f"  - {error}")

@cli.command()
@click.option('--customer-id', help='Specific customer ID')
@click.option('--country', help='Country code filter')
@click.option('--format', type=click.Choice(['csv', 'json', 'xlsx']), default='csv')
@click.option('--output', help='Output file path')
def export_data(customer_id: str, country: str, format: str, output: str):
    """Export phone data"""
    filters = {}
    if customer_id:
        filters['customer_id'] = customer_id
    if country:
        filters['country_code'] = country
    
    click.echo(f"Exporting data with filters: {filters}")
    
    result = asyncio.run(export_phone_data(filters, format, output))
    
    click.echo(f"Exported {result['record_count']} records to {result['file_path']}")

if __name__ == '__main__':
    cli()
```

### Testing Comprehensive

#### Tests de Integración
```python
import pytest
import asyncio
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

@pytest.fixture
async def async_client():
    """Cliente HTTP asíncrono para tests"""
    async with AsyncClient(app=app, base_url="http://test") as client:
        yield client

@pytest.fixture
async def db_session():
    """Sesión de base de datos para tests"""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async_session = sessionmaker(engine, class_=AsyncSession)
    
    async with async_session() as session:
        yield session

class TestPhoneAPI:
    """Tests de integración para API de teléfonos"""
    
    async def test_get_customer_phones(self, async_client, db_session):
        """Test obtener teléfonos de cliente"""
        # Preparar datos de test
        await self.create_test_customer(db_session, "CUST001")
        
        response = await async_client.get(
            "/phones/CUST001",
            headers={"Authorization": "Bearer test-token"}
        )
        
        assert response.status_code == 200
        data = response.json()
        assert len(data) > 0
        assert data[0]["customer_id"] == "CUST001"
    
    async def test_validate_phone_endpoint(self, async_client):
        """Test endpoint de validación"""
        response = await async_client.post(
            "/phones/validate",
            params={
                "phone_number": "+573001234567",
                "country_code": "CO"
            },
            headers={"Authorization": "Bearer test-token"}
        )
        
        assert response.status_code == 200
        data = response.json()
        assert "is_valid" in data
        assert "confidence_score" in data
    
    async def test_search_phones(self, async_client, db_session):
        """Test búsqueda avanzada"""
        await self.create_test_data(db_session)
        
        search_query = {
            "customer_ids": ["CUST001", "CUST002"],
            "phone_types": ["mobile"],
            "verified_only": True,
            "limit": 50
        }
        
        response = await async_client.post(
            "/phones/search",
            json=search_query,
            headers={"Authorization": "Bearer test-token"}
        )
        
        assert response.status_code == 200
        data = response.json()
        assert all(record["phone_type"] == "mobile" for record in data)
        assert all(record["is_verified"] for record in data)
    
    async def create_test_customer(self, session, customer_id):
        """Crea datos de test para un cliente"""
        # Implementar creación de datos de test
        pass
    
    async def create_test_data(self, session):
        """Crea conjunto completo de datos de test"""
        # Implementar creación de datos de test
        pass

class TestDataQuality:
    """Tests de calidad de datos"""
    
    def test_phone_normalization(self):
        """Test normalización de teléfonos"""
        service = PhoneNormalizationService()
        
        test_cases = [
            ("+57 300 123 4567", "+573001234567"),
            ("300-123-4567", "+573001234567"),
            ("(300) 123 4567", "+573001234567"),
            ("573001234567", "+573001234567")
        ]
        
        for input_phone, expected in test_cases:
            result = service.normalize_phone(input_phone, "CO")
            assert result["normalized"] == expected
    
    def test_deduplication_algorithm(self):
        """Test algoritmo de deduplicación"""
        engine = AdvancedDeduplicationEngine()
        
        test_records = [
            {"phone_number": "+573001234567", "customer_id": "CUST001"},
            {"phone_number": "+57 300 123 4567", "customer_id": "CUST001"},
            {"phone_number": "+573009876543", "customer_id": "CUST002"}
        ]
        
        duplicates = engine.find_duplicates(test_records)
        assert len(duplicates) == 1  # Un grupo de duplicados
        assert len(duplicates[0]) == 2  # Dos registros duplicados
    
    def test_validation_rules(self):
        """Test reglas de validación"""
        rules = PhoneValidationRules()
        
        # Test números válidos
        valid_phones = [
            "+573001234567",
            "+5711234567",
            "+14155552671"
        ]
        
        for phone in valid_phones:
            assert rules.validate_format(phone)
        
        # Test números inválidos
        invalid_phones = [
            "123",
            "+57123",
            "abc123def",
            ""
        ]
        
        for phone in invalid_phones:
            assert not rules.validate_format(phone)

class TestPerformance:
    """Tests de rendimiento"""
    
    @pytest.mark.asyncio
    async def test_api_response_time(self, async_client):
        """Test tiempo de respuesta de API"""
        import time
        
        start_time = time.time()
        response = await async_client.get(
            "/phones/CUST001",
            headers={"Authorization": "Bearer test-token"}
        )
        end_time = time.time()
        
        response_time = end_time - start_time
        assert response_time < 0.5  # Menos de 500ms
        assert response.status_code == 200
    
    def test_bulk_processing_performance(self):
        """Test rendimiento de procesamiento masivo"""
        # Generar dataset de test
        test_records = self.generate_test_records(10000)
        
        start_time = time.time()
        processed_records = process_phone_batch(test_records)
        end_time = time.time()
        
        processing_time = end_time - start_time
        records_per_second = len(test_records) / processing_time
        
        assert records_per_second > 1000  # Al menos 1000 registros/segundo
        assert len(processed_records) == len(test_records)
    
    def generate_test_records(self, count: int) -> List[Dict]:
        """Genera registros de test para pruebas de rendimiento"""
        import random
        
        records = []
        for i in range(count):
            record = {
                "customer_id": f"CUST{i:06d}",
                "phone_number": f"+57300{random.randint(1000000, 9999999)}",
                "phone_type": random.choice(["mobile", "fixed"]),
                "country_code": "CO"
            }
            records.append(record)
        
        return records
```

### Documentación Automatizada

#### Generación de Documentación de APIs
```python
from fastapi.openapi.utils import get_openapi

def custom_openapi():
    """Genera documentación OpenAPI personalizada"""
    if app.openapi_schema:
        return app.openapi_schema
    
    openapi_schema = get_openapi(
        title="Phone Dataset API",
        version="1.0.0",
        description="""
        API para acceso y gestión del dataset de números telefónicos de clientes.
        
        ## Características
        
        * **Validación en tiempo real** de números telefónicos
        * **Búsqueda avanzada** con múltiples filtros
        * **Normalización automática** a formato E.164
        * **Métricas de calidad** integradas
        * **Trazabilidad completa** de cambios
        
        ## Autenticación
        
        Todas las operaciones requieren un token Bearer válido en el header Authorization.
        
        ## Límites de Rate
        
        * 1000 requests por minuto para operaciones de lectura
        * 100 requests por minuto para operaciones de escritura
        """,
        routes=app.routes,
    )
    
    # Añadir información de contacto y licencia
    openapi_schema["info"]["contact"] = {
        "name": "Data Engineering Team",
        "email": "data-engineering@company.com",
        "url": "https://company.com/data-team"
    }
    
    openapi_schema["info"]["license"] = {
        "name": "MIT",
        "url": "https://opensource.org/licenses/MIT"
    }
    
    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.openapi = custom_openapi
```

#### Documentación de Esquemas de Datos
```python
def generate_data_dictionary():
    """Genera diccionario de datos automáticamente"""
    return {
        "customer_phones": {
            "description": "Tabla principal de números telefónicos de clientes",
            "columns": {
                "phone_id": {
                    "type": "SERIAL",
                    "description": "Identificador único del registro telefónico",
                    "constraints": ["PRIMARY KEY", "NOT NULL"]
                },
                "customer_id": {
                    "type": "VARCHAR(50)",
                    "description": "Identificador del cliente propietario del teléfono",
                    "constraints": ["NOT NULL", "FOREIGN KEY"]
                },
                "phone_number": {
                    "type": "VARCHAR(20)",
                    "description": "Número telefónico normalizado en formato E.164",
                    "constraints": ["NOT NULL", "UNIQUE per customer"],
                    "format": "+[country_code][number]",
                    "example": "+573001234567"
                },
                "phone_type": {
                    "type": "VARCHAR(20)",
                    "description": "Tipo de número telefónico",
                    "constraints": ["NOT NULL"],
                    "valid_values": ["mobile", "fixed", "fax", "voip"]
                },
                "country_code": {
                    "type": "CHAR(2)",
                    "description": "Código de país ISO 3166-1 alpha-2",
                    "constraints": ["NOT NULL"],
                    "example": "CO"
                },
                "carrier": {
                    "type": "VARCHAR(50)",
                    "description": "Operador de telecomunicaciones",
                    "constraints": ["NULLABLE"],
                    "example": "Claro"
                },
                "is_primary": {
                    "type": "BOOLEAN",
                    "description": "Indica si es el teléfono principal del cliente",
                    "constraints": ["DEFAULT FALSE"]
                },
                "is_verified": {
                    "type": "BOOLEAN",
                    "description": "Indica si el teléfono ha sido verificado",
                    "constraints": ["DEFAULT FALSE"]
                },
                "confidence_score": {
                    "type": "DECIMAL(3,2)",
                    "description": "Puntuación de confianza del número (0.00-1.00)",
                    "constraints": ["NULLABLE", "CHECK >= 0 AND <= 1"]
                },
                "source_system": {
                    "type": "VARCHAR(50)",
                    "description": "Sistema fuente del dato",
                    "constraints": ["NOT NULL"],
                    "valid_values": ["crm_salesforce", "call_center", "web_forms", "mobile_app"]
                }
            },
            "indexes": [
                {
                    "name": "idx_customer_phones_customer_id",
                    "columns": ["customer_id"],
                    "type": "btree"
                },
                {
                    "name": "idx_customer_phones_phone_number",
                    "columns": ["phone_number"],
                    "type": "btree"
                }
            ]
        }
    }
```

## Consideraciones de Escalabilidad

### Estrategias de Particionamiento
```sql
-- Particionamiento por país para optimizar consultas geográficas
CREATE TABLE customer_phones_co PARTITION OF customer_phones
FOR VALUES IN ('CO');

CREATE TABLE customer_phones_us PARTITION OF customer_phones
FOR VALUES IN ('US');

CREATE TABLE customer_phones_mx PARTITION OF customer_phones
FOR VALUES IN ('MX');

-- Particionamiento temporal para auditoría
CREATE TABLE phone_audit_trail_2024 PARTITION OF phone_audit_trail
FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

-- Índices específicos por partición
CREATE INDEX idx_phones_co_customer ON customer_phones_co(customer_id);
CREATE INDEX idx_phones_co_type ON customer_phones_co(phone_type);
```

### Optimización de Consultas
```python
class QueryOptimizer:
    """Optimizador de consultas para el dataset"""
    
    def __init__(self):
        self.query_cache = {}
        self.execution_stats = {}
    
    async def get_customer_phones_optimized(self, customer_id: str, filters: Dict = None):
        """Consulta optimizada para teléfonos de cliente"""
        
        # Construir query con hints de optimización
        query = """
        SELECT /*+ INDEX(customer_phones, idx_customer_phones_customer_id) */
               phone_id, customer_id, phone_number, phone_type,
               country_code, carrier, is_primary, is_verified,
               confidence_score, source_system
        FROM customer_phones 
        WHERE customer_id = :customer_id
        """
        
        # Añadir filtros dinámicos
        if filters:
            if filters.get('verified_only'):
                query += " AND is_verified = true"
            if filters.get('phone_type'):
                query += " AND phone_type = :phone_type"
        
        # Ordenar por teléfono primario primero
        query += " ORDER BY is_primary DESC, updated_at DESC"
        
        # Ejecutar con parámetros
        return await self.execute_query(query, {
            'customer_id': customer_id,
            **filters
        })
    
    async def batch_lookup_phones(self, phone_numbers: List[str]):
        """Búsqueda optimizada en lote"""
        
        # Usar ANY para búsqueda eficiente en lote
        query = """
        SELECT phone_number, customer_id, phone_type, carrier, is_verified
        FROM customer_phones
        WHERE phone_number = ANY(:phone_numbers)
        """
        
        return await self.execute_query(query, {
            'phone_numbers': phone_numbers
        })
```

### Caching Strategy
```python
import redis
import json
from datetime import timedelta

class PhoneCacheManager:
    """Gestor de caché para optimizar acceso a datos"""
    
    def __init__(self):
        self.redis_client = redis.Redis(
            host='redis',
            port=6379,
            decode_responses=True
        )
        self.default_ttl = 3600  # 1 hora
    
    async def get_customer_phones_cached(self, customer_id: str):
        """Obtiene teléfonos con caché"""
        cache_key = f"customer_phones:{customer_id}"
        
        # Intentar obtener del caché
        cached_data = self.redis_client.get(cache_key)
        if cached_data:
            return json.loads(cached_data)
        
        # Si no está en caché, consultar BD
        phones = await self.get_customer_phones_from_db(customer_id)
        
        # Guardar en caché
        self.redis_client.setex(
            cache_key,
            self.default_ttl,
            json.dumps(phones, default=str)
        )
        
        return phones
    
    def invalidate_customer_cache(self, customer_id: str):
        """Invalida caché de un cliente específico"""
        cache_key = f"customer_phones:{customer_id}"
        self.redis_client.delete(cache_key)
    
    def warm_cache(self, customer_ids: List[str]):
        """Pre-carga caché para clientes específicos"""
        for customer_id in customer_ids:
            asyncio.create_task(self.get_customer_phones_cached(customer_id))
```

---

**Nota**: Esta arquitectura está diseñada para escalar desde MVP hasta enterprise-grade, manteniendo siempre los más altos estándares de calidad y seguridad de datos.
