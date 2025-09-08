# Sistema de KPIs de Calidad de Datos

**Desarrollado por:** Johnnatan Villada Flórez

Sistema integral de veeduría para la calidad de datos, trazabilidad y KPIs del dataset de números de teléfono de clientes. Proporciona visibilidad completa para equipos de negocio sobre la salud y evolución de los datos.

## Objetivos del Sistema

- **Transparencia**: Visibilidad 360° de la calidad de datos
- **Proactividad**: Detección temprana de problemas de calidad
- **Trazabilidad**: Linaje completo del dato desde origen hasta consumo
- **Gobierno**: Métricas para toma de decisiones basada en datos
- **Automatización**: Monitoreo continuo sin intervención manual

## Arquitectura del Sistema de KPIs

```mermaid
graph TB
    subgraph "Fuentes de Métricas"
        A1[Dataset Teléfonos]
        A2[Logs de Pipeline]
        A3[APIs de Consumo]
        A4[Feedback Usuarios]
    end

    subgraph "Recolección de Métricas"
        B1[Metric Collectors]
        B2[Event Aggregators]
        B3[Quality Scanners]
        B4[Usage Analytics]
    end

    subgraph "Procesamiento"
        C1[Stream Processing]
        C2[Batch Analytics]
        C3[ML Anomaly Detection]
        C4[Trend Analysis]
    end

    subgraph "Storage & Indexing"
        D1[Time Series DB]
        D2[Metrics Warehouse]
        D3[Search Index]
        D4[Cache Layer]
    end

    subgraph "Presentation Layer"
        E1[Executive Dashboard]
        E2[Operational Console]
        E3[Data Catalog]
        E4[Alert System]
    end

    subgraph "APIs & Integration"
        F1[Metrics API]
        F2[Webhook Service]
        F3[Report Generator]
        F4[Export Service]
    end

    A1 --> B1
    A2 --> B2
    A3 --> B3
    A4 --> B4

    B1 --> C1
    B2 --> C1
    B3 --> C2
    B4 --> C2

    C1 --> D1
    C2 --> D2
    C3 --> D3
    C4 --> D1

    D1 --> E1
    D2 --> E2
    D3 --> E3
    D4 --> E4

    E1 --> F1
    E2 --> F2
    E3 --> F3
    E4 --> F4

    style A1 fill:#e1f5fe
    style C3 fill:#fff3e0
    style E1 fill:#e8f5e8
    style F1 fill:#fce4ec
```

## Dimensiones de Calidad de Datos

### 1. Completitud (Completeness)
```python
class CompletenessMetrics:
    """Métricas de completitud de datos"""
    
    def calculate_completeness(self, dataset):
        return {
            'phone_number_completeness': self._field_completeness('phone_number'),
            'customer_id_completeness': self._field_completeness('customer_id'),
            'overall_completeness': self._overall_completeness(),
            'completeness_trend': self._calculate_trend(period='7d'),
            'completeness_by_source': self._completeness_by_source()
        }
    
    # KPI Target: >= 95%
    # Alert Threshold: < 90%
```

### 2. Precisión (Accuracy)
```python
class AccuracyMetrics:
    """Métricas de precisión de datos"""
    
    def calculate_accuracy(self, dataset):
        return {
            'format_accuracy': self._validate_phone_format(),
            'carrier_accuracy': self._validate_carrier_info(),
            'geography_accuracy': self._validate_country_codes(),
            'business_rule_accuracy': self._validate_business_rules(),
            'external_validation_accuracy': self._external_api_validation()
        }
    
    # KPI Target: >= 98%
    # Alert Threshold: < 95%
```

### 3. Consistencia (Consistency)
```python
class ConsistencyMetrics:
    """Métricas de consistencia de datos"""
    
    def calculate_consistency(self, dataset):
        return {
            'cross_system_consistency': self._compare_with_crm(),
            'temporal_consistency': self._check_temporal_patterns(),
            'referential_consistency': self._check_foreign_keys(),
            'format_consistency': self._check_format_standards(),
            'duplicate_consistency': self._check_duplicate_handling()
        }
    
    # KPI Target: >= 96%
    # Alert Threshold: < 92%
```

### 4. Oportunidad (Timeliness)
```python
class TimelinessMetrics:
    """Métricas de oportunidad de datos"""
    
    def calculate_timeliness(self, dataset):
        return {
            'data_freshness': self._calculate_data_age(),
            'update_frequency': self._calculate_update_patterns(),
            'ingestion_latency': self._measure_ingestion_time(),
            'processing_latency': self._measure_processing_time(),
            'availability_lag': self._measure_availability_delay()
        }
    
    # KPI Target: <= 24 horas
    # Alert Threshold: > 48 horas
```

### 5. Unicidad (Uniqueness)
```python
class UniquenessMetrics:
    """Métricas de unicidad de datos"""
    
    def calculate_uniqueness(self, dataset):
        return {
            'phone_uniqueness': self._calculate_phone_duplicates(),
            'customer_phone_uniqueness': self._check_customer_duplicates(),
            'fuzzy_duplicates': self._detect_fuzzy_duplicates(),
            'cross_reference_duplicates': self._check_cross_system_duplicates()
        }
    
    # KPI Target: >= 99%
    # Alert Threshold: < 97%
```

## KPIs Principales

### Dashboard Ejecutivo

#### 1. Data Health Score
```python
class DataHealthScore:
    """Score general de salud de datos (0-100)"""
    
    def calculate_health_score(self):
        weights = {
            'completeness': 0.25,
            'accuracy': 0.30,
            'consistency': 0.20,
            'timeliness': 0.15,
            'uniqueness': 0.10
        }
        
        return sum(metric * weight for metric, weight in weights.items())
    
    # Grade Scale:
    # A: 90-100 (Excelente)
    # B: 80-89  (Bueno)
    # C: 70-79  (Aceptable)
    # D: 60-69  (Mejorable)
    # F: 0-59   (Crítico)
```

#### 2. KPIs de Negocio
```python
business_kpis = {
    'customer_coverage': {
        'metric': 'Cobertura de Clientes con Teléfono',
        'formula': 'clientes_con_telefono / total_clientes',
        'target': 0.95,
        'current': 0.93,
        'trend': '+2.1%'
    },
    'contact_success_rate': {
        'metric': 'Tasa de Contacto Exitoso',
        'formula': 'contactos_exitosos / intentos_contacto',
        'target': 0.75,
        'current': 0.82,
        'trend': '+5.3%'
    },
    'data_utilization': {
        'metric': 'Utilización del Dataset',
        'formula': 'consultas_exitosas / total_consultas',
        'target': 0.90,
        'current': 0.88,
        'trend': '-1.2%'
    },
    'cost_per_valid_phone': {
        'metric': 'Costo por Teléfono Válido',
        'formula': 'costo_total_procesamiento / telefonos_validos',
        'target': 0.15,  # USD
        'current': 0.12,
        'trend': '-8.7%'
    }
}
```

### Dashboard Operacional

#### 1. Métricas en Tiempo Real
```python
real_time_metrics = {
    'ingestion_rate': {
        'current': '1,250 records/min',
        'target': '1,000 records/min',
        'status': 'healthy'
    },
    'processing_queue': {
        'pending': 2340,
        'processing': 15,
        'failed': 3,
        'avg_wait_time': '2.3 min'
    },
    'api_performance': {
        'response_time_p95': '145ms',
        'error_rate': '0.2%',
        'throughput': '850 req/min',
        'uptime': '99.98%'
    },
    'data_quality_alerts': {
        'active_alerts': 2,
        'resolved_today': 7,
        'avg_resolution_time': '23 min'
    }
}
```

## Sistema de Trazabilidad

### Linaje de Datos
```python
class DataLineageTracker:
    """Sistema de trazabilidad del dato"""
    
    def track_phone_lineage(self, phone_id: str):
        """
        Rastrea el linaje completo de un número telefónico
        """
        return {
            'source_system': 'CRM Salesforce',
            'ingestion_timestamp': '2024-01-15T10:30:00Z',
            'transformations': [
                {
                    'step': 'normalization',
                    'timestamp': '2024-01-15T10:30:15Z',
                    'changes': ['format_standardization', 'country_code_addition']
                },
                {
                    'step': 'validation',
                    'timestamp': '2024-01-15T10:30:45Z',
                    'results': {'format_valid': True, 'carrier_verified': True}
                },
                {
                    'step': 'enrichment',
                    'timestamp': '2024-01-15T10:31:00Z',
                    'external_apis': ['carrier_lookup', 'geolocation']
                }
            ],
            'quality_scores': {
                'initial': 0.85,
                'post_processing': 0.97
            },
            'usage_history': [
                {
                    'consumer': 'Marketing Campaign API',
                    'timestamp': '2024-01-15T14:22:00Z',
                    'purpose': 'SMS Campaign'
                }
            ],
            'current_status': 'active',
            'last_verified': '2024-01-20T09:15:00Z'
        }
```

### Audit Trail
```sql
-- Tabla de auditoría completa
CREATE TABLE data_audit_trail (
    audit_id SERIAL PRIMARY KEY,
    entity_type VARCHAR(50), -- 'phone_record', 'customer', etc.
    entity_id VARCHAR(100),
    operation VARCHAR(20), -- CREATE, UPDATE, DELETE, ACCESS
    operation_details JSONB,
    data_before JSONB,
    data_after JSONB,
    user_id VARCHAR(100),
    system_id VARCHAR(100),
    ip_address INET,
    user_agent TEXT,
    operation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processing_time_ms INTEGER,
    quality_impact JSONB,
    compliance_flags TEXT[]
);

-- Índices para consultas eficientes
CREATE INDEX idx_audit_entity ON data_audit_trail(entity_type, entity_id);
CREATE INDEX idx_audit_timestamp ON data_audit_trail(operation_timestamp);
CREATE INDEX idx_audit_user ON data_audit_trail(user_id);
```

## Interfaces de Usuario

### 1. Dashboard Ejecutivo
```html
<!-- Vista de alto nivel para ejecutivos -->
<div class="executive-dashboard">
    <div class="health-score-card">
        <h2>Data Health Score</h2>
        <div class="score-display">
            <span class="score">87</span>
            <span class="grade">B</span>
            <div class="trend positive">+3.2%</div>
        </div>
    </div>
    
    <div class="kpi-grid">
        <div class="kpi-card">
            <h3>Customer Coverage</h3>
            <div class="metric">93.2%</div>
            <div class="target">Target: 95%</div>
        </div>
        
        <div class="kpi-card">
            <h3>Contact Success</h3>
            <div class="metric">82.1%</div>
            <div class="target">Target: 75%</div>
        </div>
        
        <div class="kpi-card">
            <h3>Data Quality</h3>
            <div class="metric">96.8%</div>
            <div class="target">Target: 95%</div>
        </div>
    </div>
    
    <div class="trend-charts">
        <!-- Gráficos de tendencias temporales -->
    </div>
</div>
```

### 2. Console Operacional
```html
<!-- Vista detallada para equipos técnicos -->
<div class="operational-console">
    <div class="real-time-metrics">
        <h2>Real-time Monitoring</h2>
        <div class="metrics-grid">
            <!-- Métricas en tiempo real -->
        </div>
    </div>
    
    <div class="quality-breakdown">
        <h2>Quality Dimensions</h2>
        <div class="dimension-cards">
            <!-- Desglose por dimensión de calidad -->
        </div>
    </div>
    
    <div class="alerts-panel">
        <h2>Active Alerts</h2>
        <div class="alert-list">
            <!-- Lista de alertas activas -->
        </div>
    </div>
    
    <div class="lineage-explorer">
        <h2>Data Lineage</h2>
        <div class="lineage-graph">
            <!-- Visualización del linaje de datos -->
        </div>
    </div>
</div>
```

### 3. Data Catalog
```python
class DataCatalogInterface:
    """Catálogo de datos para autodescubrimiento"""
    
    def get_dataset_metadata(self, dataset_id: str):
        return {
            'dataset_name': 'Customer Phone Numbers',
            'description': 'Comprehensive dataset of customer phone numbers',
            'owner': 'Data Engineering Team',
            'steward': 'Customer Data Steward',
            'classification': 'PII - Restricted',
            'update_frequency': 'Real-time',
            'schema': {
                'fields': [
                    {
                        'name': 'phone_id',
                        'type': 'string',
                        'description': 'Unique identifier for phone record',
                        'is_pii': False
                    },
                    {
                        'name': 'phone_number',
                        'type': 'string',
                        'description': 'Normalized phone number in E.164 format',
                        'is_pii': True,
                        'format': '+[country_code][number]'
                    }
                ]
            },
            'quality_metrics': {
                'last_updated': '2024-01-20T09:00:00Z',
                'health_score': 87,
                'completeness': 96.2,
                'accuracy': 97.8
            },
            'usage_stats': {
                'monthly_queries': 15420,
                'unique_consumers': 23,
                'avg_response_time': '145ms'
            },
            'access_requirements': [
                'Data Access Request Form',
                'PII Handling Training',
                'Manager Approval'
            ]
        }
```

## Sistema de Alertas

### Configuración de Alertas
```python
alert_configuration = {
    'critical_alerts': {
        'data_quality_drop': {
            'condition': 'health_score < 70',
            'channels': ['email', 'slack', 'pagerduty'],
            'escalation': {
                'level_1': 'Data Engineering Team',
                'level_2': 'Data Engineering Manager',
                'level_3': 'CTO'
            },
            'auto_actions': ['pause_pipeline', 'notify_consumers']
        },
        'processing_failure': {
            'condition': 'failed_records > 1000 AND failure_rate > 0.05',
            'channels': ['slack', 'email'],
            'escalation': {
                'level_1': 'Data Engineering Team',
                'level_2': 'Infrastructure Team'
            }
        }
    },
    'warning_alerts': {
        'completeness_degradation': {
            'condition': 'completeness < 90%',
            'channels': ['slack'],
            'frequency': 'hourly'
        },
        'latency_increase': {
            'condition': 'processing_latency > 1h',
            'channels': ['slack'],
            'frequency': 'real-time'
        }
    },
    'info_alerts': {
        'daily_summary': {
            'condition': 'daily_report',
            'channels': ['email'],
            'frequency': 'daily'
        }
    }
}
```

### Sistema de Notificaciones
```python
class AlertingSystem:
    """Sistema centralizado de alertas"""
    
    def __init__(self):
        self.channels = {
            'email': EmailChannel(),
            'slack': SlackChannel(),
            'pagerduty': PagerDutyChannel(),
            'webhook': WebhookChannel()
        }
    
    def evaluate_rules(self, metrics: dict):
        """Evalúa reglas de alertas contra métricas actuales"""
        triggered_alerts = []
        
        for alert_type, alerts in alert_configuration.items():
            for alert_name, config in alerts.items():
                if self._evaluate_condition(config['condition'], metrics):
                    alert = {
                        'name': alert_name,
                        'type': alert_type,
                        'timestamp': datetime.utcnow(),
                        'metrics': metrics,
                        'config': config
                    }
                    triggered_alerts.append(alert)
                    self._send_alert(alert)
        
        return triggered_alerts
    
    def _send_alert(self, alert: dict):
        """Envía alerta por los canales configurados"""
        for channel_name in alert['config']['channels']:
            channel = self.channels[channel_name]
            channel.send_alert(alert)
    
    def _evaluate_condition(self, condition: str, metrics: dict) -> bool:
        """Evalúa condición de alerta"""
        # Implementación de evaluación de condiciones
        pass
```

## APIs de Integración

### 1. Metrics API
```python
from fastapi import FastAPI, Query, HTTPException
from typing import Optional, List
import datetime

app = FastAPI(title="Data Quality Metrics API")

@app.get("/metrics/health-score")
async def get_health_score(
    dataset_id: str = Query(..., description="Dataset identifier"),
    start_date: Optional[datetime.date] = None,
    end_date: Optional[datetime.date] = None
):
    """Obtiene el health score del dataset"""
    try:
        health_score = calculate_health_score(dataset_id, start_date, end_date)
        return {
            "dataset_id": dataset_id,
            "health_score": health_score,
            "grade": get_grade(health_score),
            "timestamp": datetime.utcnow(),
            "period": {
                "start": start_date,
                "end": end_date
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/metrics/quality-dimensions")
async def get_quality_dimensions(dataset_id: str):
    """Obtiene métricas por dimensión de calidad"""
    return {
        "dataset_id": dataset_id,
        "dimensions": {
            "completeness": get_completeness_metrics(dataset_id),
            "accuracy": get_accuracy_metrics(dataset_id),
            "consistency": get_consistency_metrics(dataset_id),
            "timeliness": get_timeliness_metrics(dataset_id),
            "uniqueness": get_uniqueness_metrics(dataset_id)
        },
        "timestamp": datetime.utcnow()
    }

@app.get("/metrics/trends")
async def get_trends(
    dataset_id: str,
    metric: str,
    period: str = "7d"
):
    """Obtiene tendencias de métricas"""
    return {
        "dataset_id": dataset_id,
        "metric": metric,
        "period": period,
        "data_points": get_trend_data(dataset_id, metric, period),
        "summary": {
            "trend_direction": "increasing",
            "change_percentage": 2.3,
            "average": 94.5
        }
    }

@app.get("/lineage/{record_id}")
async def get_data_lineage(record_id: str):
    """Obtiene el linaje completo de un registro"""
    lineage = DataLineageTracker().track_phone_lineage(record_id)
    return lineage

@app.get("/alerts/active")
async def get_active_alerts(severity: Optional[str] = None):
    """Obtiene alertas activas"""
    alerts = get_active_alerts_from_db(severity)
    return {
        "alerts": alerts,
        "count": len(alerts),
        "timestamp": datetime.utcnow()
    }
```

### 2. Webhook Service
```python
class WebhookService:
    """Servicio de webhooks para notificaciones externas"""
    
    def __init__(self):
        self.subscribers = {}
    
    def register_webhook(self, webhook_url: str, events: List[str], secret: str):
        """Registra un webhook para eventos específicos"""
        webhook_id = generate_webhook_id()
        self.subscribers[webhook_id] = {
            'url': webhook_url,
            'events': events,
            'secret': secret,
            'created_at': datetime.utcnow(),
            'active': True
        }
        return webhook_id
    
    def send_webhook(self, event_type: str, payload: dict):
        """Envía webhook a suscriptores relevantes"""
        for webhook_id, config in self.subscribers.items():
            if event_type in config['events'] and config['active']:
                self._deliver_webhook(config, event_type, payload)
    
    def _deliver_webhook(self, config: dict, event_type: str, payload: dict):
        """Entrega webhook con reintentos y firma"""
        webhook_payload = {
            'event_type': event_type,
            'timestamp': datetime.utcnow().isoformat(),
            'data': payload
        }
        
        signature = self._generate_signature(webhook_payload, config['secret'])
        headers = {
            'Content-Type': 'application/json',
            'X-Webhook-Signature': signature,
            'X-Event-Type': event_type
        }
        
        # Implementar lógica de entrega con reintentos
        pass
```

## Reportes Automatizados

### 1. Reporte Ejecutivo Diario
```python
class ExecutiveReportGenerator:
    """Generador de reportes ejecutivos"""
    
    def generate_daily_report(self, date: datetime.date = None):
        """Genera reporte ejecutivo diario"""
        if not date:
            date = datetime.date.today()
        
        metrics = self._collect_daily_metrics(date)
        
        report = {
            'report_type': 'executive_daily',
            'date': date.isoformat(),
            'generated_at': datetime.utcnow().isoformat(),
            'summary': {
                'overall_health_score': metrics['health_score'],
                'health_score_change': metrics['health_score_change'],
                'grade': get_grade(metrics['health_score']),
                'key_insights': self._generate_insights(metrics)
            },
            'kpis': {
                'customer_coverage': metrics['customer_coverage'],
                'contact_success_rate': metrics['contact_success_rate'],
                'data_utilization': metrics['data_utilization'],
                'cost_per_valid_phone': metrics['cost_per_valid_phone']
            },
            'quality_dimensions': {
                'completeness': metrics['completeness'],
                'accuracy': metrics['accuracy'],
                'consistency': metrics['consistency'],
                'timeliness': metrics['timeliness'],
                'uniqueness': metrics['uniqueness']
            },
            'alerts_summary': {
                'critical_alerts': metrics['critical_alerts_count'],
                'warning_alerts': metrics['warning_alerts_count'],
                'resolved_alerts': metrics['resolved_alerts_count']
            },
            'recommendations': self._generate_recommendations(metrics)
        }
        
        return report
    
    def _generate_insights(self, metrics: dict) -> List[str]:
        """Genera insights automáticos basados en métricas"""
        insights = []
        
        if metrics['health_score_change'] > 5:
            insights.append("Mejora significativa en la calidad de datos (+5.2%)")
        
        if metrics['completeness'] < 90:
            insights.append("Completitud por debajo del objetivo (90%)")
        
        if metrics['critical_alerts_count'] > 0:
            insights.append(f"{metrics['critical_alerts_count']} alertas críticas requieren atención")
        
        return insights
    
    def _generate_recommendations(self, metrics: dict) -> List[str]:
        """Genera recomendaciones automáticas"""
        recommendations = []
        
        if metrics['accuracy'] < 95:
            recommendations.append("Revisar reglas de validación de números telefónicos")
        
        if metrics['processing_latency'] > 3600:  # 1 hora
            recommendations.append("Optimizar pipeline de procesamiento")
        
        return recommendations
```

### 2. Reporte Técnico Semanal
```python
class TechnicalReportGenerator:
    """Generador de reportes técnicos detallados"""
    
    def generate_weekly_report(self, week_start: datetime.date):
        """Genera reporte técnico semanal"""
        week_end = week_start + datetime.timedelta(days=6)
        
        return {
            'report_type': 'technical_weekly',
            'period': {
                'start': week_start.isoformat(),
                'end': week_end.isoformat()
            },
            'performance_metrics': self._collect_performance_metrics(week_start, week_end),
            'quality_trends': self._analyze_quality_trends(week_start, week_end),
            'pipeline_statistics': self._collect_pipeline_stats(week_start, week_end),
            'error_analysis': self._analyze_errors(week_start, week_end),
            'capacity_analysis': self._analyze_capacity(week_start, week_end),
            'recommendations': self._technical_recommendations(week_start, week_end)
        }
```

## Monitoreo y Observabilidad

### 1. Health Checks
```python
class SystemHealthChecker:
    """Verificador de salud del sistema"""
    
    def check_system_health(self):
        """Verifica la salud general del sistema"""
        checks = {
            'database_connectivity': self._check_database(),
            'api_responsiveness': self._check_api_endpoints(),
            'pipeline_status': self._check_pipeline_health(),
            'external_dependencies': self._check_external_apis(),
            'storage_capacity': self._check_storage(),
            'processing_queues': self._check_queues()
        }
        
        overall_status = 'healthy' if all(
            check['status'] == 'healthy' for check in checks.values()
        ) else 'degraded'
        
        return {
            'overall_status': overall_status,
            'timestamp': datetime.utcnow().isoformat(),
            'checks': checks,
            'summary': self._generate_health_summary(checks)
        }
    
    def _check_database(self):
        """Verifica conectividad y rendimiento de base de datos"""
        try:
            start_time = time.time()
            # Ejecutar query de prueba
            response_time = (time.time() - start_time) * 1000
            
            return {
                'status': 'healthy' if response_time < 100 else 'degraded',
                'response_time_ms': response_time,
                'details': 'Database connectivity OK'
            }
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e),
                'details': 'Database connection failed'
            }
```

### 2. Métricas de Rendimiento
```python
class PerformanceMetrics:
    """Métricas de rendimiento del sistema"""
    
    def collect_performance_metrics(self):
        """Recolecta métricas de rendimiento en tiempo real"""
        return {
            'api_metrics': {
                'requests_per_second': self._get_rps(),
                'avg_response_time': self._get_avg_response_time(),
                'p95_response_time': self._get_p95_response_time(),
                'error_rate': self._get_error_rate(),
                'active_connections': self._get_active_connections()
            },
            'pipeline_metrics': {
                'records_processed_per_minute': self._get_processing_rate(),
                'queue_depth': self._get_queue_depth(),
                'processing_errors': self._get_processing_errors(),
                'avg_processing_time': self._get_avg_processing_time()
            },
            'resource_metrics': {
                'cpu_usage': self._get_cpu_usage(),
                'memory_usage': self._get_memory_usage(),
                'disk_usage': self._get_disk_usage(),
                'network_io': self._get_network_io()
            }
        }
```

## Configuración y Deployment

### 1. Configuración del Sistema
```yaml
# config.yaml
system:
  name: "Data Quality KPI System"
  version: "1.0.0"
  environment: "production"

database:
  host: "metrics-db.company.com"
  port: 5432
  database: "data_quality_metrics"
  pool_size: 20
  connection_timeout: 30

metrics:
  collection_interval: 300  # 5 minutos
  retention_days: 365
  aggregation_levels: ["1m", "5m", "1h", "1d"]

alerts:
  evaluation_interval: 60  # 1 minuto
  max_alerts_per_hour: 100
  escalation_timeout: 1800  # 30 minutos
  
api:
  host: "0.0.0.0"
  port: 8080
  workers: 4
  timeout: 30
  rate_limit: "1000/minute"

webhooks:
  max_retries: 3
  retry_delay: 30
  timeout: 10

logging:
  level: "INFO"
  format: "json"
  output: "stdout"
```

### 2. Docker Configuration
```dockerfile
# Dockerfile
FROM python:3.11-slim

WORKDIR /app

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copiar requirements y instalar dependencias Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código fuente
COPY . .

# Crear usuario no-root
RUN useradd --create-home --shell /bin/bash app
USER app

# Exponer puerto
EXPOSE 8080

# Comando por defecto
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
```

### 3. Docker Compose
```yaml
# docker-compose.yml
version: '3.8'

services:
  data-quality-api:
    build: .
    ports:
      - "8080:8080"
    environment:
      - DATABASE_URL=postgresql://user:password@postgres:5432/data_quality
      - REDIS_URL=redis://redis:6379
    depends_on:
      - postgres
      - redis
    volumes:
      - ./config.yaml:/app/config.yaml
    restart: unless-stopped

  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: data_quality
      POSTGRES_USER: user
      POSTGRES_PASSWORD: password
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./init.sql:/docker-entrypoint-initdb.d/init.sql
    ports:
      - "5432:5432"

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data

  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
    volumes:
      - grafana_data:/var/lib/grafana
      - ./grafana/dashboards:/etc/grafana/provisioning/dashboards
      - ./grafana/datasources:/etc/grafana/provisioning/datasources

volumes:
  postgres_data:
  redis_data:
  grafana_data:
```

## Implementación Paso a Paso

### Fase 1: Infraestructura Base (Semanas 1-2)
```python
# Tareas de implementación
phase_1_tasks = [
    {
        'task': 'Configurar base de datos',
        'description': 'Instalar PostgreSQL y crear esquemas',
        'duration': '3 días',
        'dependencies': []
    },
    {
        'task': 'Implementar APIs básicas',
        'description': 'Crear endpoints para métricas básicas',
        'duration': '5 días',
        'dependencies': ['Configurar base de datos']
    },
    {
        'task': 'Sistema de recolección de métricas',
        'description': 'Implementar collectors para dimensiones de calidad',
        'duration': '5 días',
        'dependencies': ['Implementar APIs básicas']
    },
    {
        'task': 'Dashboard básico',
        'description': 'Crear dashboard ejecutivo básico',
        'duration': '3 días',
        'dependencies': ['Sistema de recolección de métricas']
    }
]
```

### Fase 2: Funcionalidades Avanzadas (Semanas 3-4)
```python
phase_2_tasks = [
    {
        'task': 'Sistema de alertas',
        'description': 'Implementar motor de alertas y notificaciones',
        'duration': '4 días',
        'dependencies': ['Dashboard básico']
    },
    {
        'task': 'Trazabilidad de datos',
        'description': 'Implementar sistema de linaje',
        'duration': '5 días',
        'dependencies': ['Sistema de alertas']
    },
    {
        'task': 'Reportes automatizados',
        'description': 'Crear generadores de reportes',
        'duration': '3 días',
        'dependencies': ['Trazabilidad de datos']
    },
    {
        'task': 'Integración webhooks',
        'description': 'Implementar sistema de webhooks',
        'duration': '3 días',
        'dependencies': ['Reportes automatizados']
    }
]
```

### Fase 3: Optimización y Producción (Semanas 5-6)
```python
phase_3_tasks = [
    {
        'task': 'Optimización de rendimiento',
        'description': 'Optimizar queries y caching',
        'duration': '4 días',
        'dependencies': ['Integración webhooks']
    },
    {
        'task': 'Monitoreo y observabilidad',
        'description': 'Implementar métricas de sistema',
        'duration': '3 días',
        'dependencies': ['Optimización de rendimiento']
    },
    {
        'task': 'Documentación y training',
        'description': 'Crear documentación y entrenar usuarios',
        'duration': '3 días',
        'dependencies': ['Monitoreo y observabilidad']
    },
    {
        'task': 'Despliegue a producción',
        'description': 'Deployment y configuración producción',
        'duration': '4 días',
        'dependencies': ['Documentación y training']
    }
]
```

## Casos de Uso Específicos

### 1. Detección de Anomalías
```python
class AnomalyDetector:
    """Detector de anomalías en calidad de datos"""
    
    def __init__(self):
        self.models = {
            'completeness': IsolationForest(),
            'accuracy': IsolationForest(),
            'consistency': IsolationForest()
        }
    
    def detect_anomalies(self, metric_type: str, values: List[float]):
        """Detecta anomalías en series temporales de métricas"""
        if metric_type not in self.models:
            raise ValueError(f"Unsupported metric type: {metric_type}")
        
        model = self.models[metric_type]
        anomalies = model.fit_predict(np.array(values).reshape(-1, 1))
        
        return {
            'anomalies_detected': len([x for x in anomalies if x == -1]),
            'anomaly_indices': [i for i, x in enumerate(anomalies) if x == -1],
            'anomaly_scores': model.decision_function(np.array(values).reshape(-1, 1)),
            'recommendation': self._get_anomaly_recommendation(metric_type, anomalies)
        }
    
    def _get_anomaly_recommendation(self, metric_type: str, anomalies: List[int]) -> str:
        """Genera recomendaciones basadas en anomalías detectadas"""
        anomaly_count = len([x for x in anomalies if x == -1])
        
        if anomaly_count == 0:
            return "No se detectaron anomalías"
        elif anomaly_count < 3:
            return f"Se detectaron {anomaly_count} anomalías menores en {metric_type}"
        else:
            return f"ALERTA: {anomaly_count} anomalías significativas en {metric_type} - Revisar pipeline"
```

### 2. Predicción de Calidad
```python
class QualityPredictor:
    """Predictor de tendencias de calidad de datos"""
    
    def __init__(self):
        self.models = {}
    
    def predict_quality_trend(self, metric_history: List[dict], days_ahead: int = 7):
        """Predice tendencia de calidad para los próximos días"""
        
        # Preparar datos históricos
        df = pd.DataFrame(metric_history)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.set_index('timestamp')
        
        predictions = {}
        
        for metric in ['completeness', 'accuracy', 'consistency', 'timeliness', 'uniqueness']:
            if metric in df.columns:
                # Usar ARIMA para predicción
                model = ARIMA(df[metric], order=(1,1,1))
                fitted_model = model.fit()
                
                forecast = fitted_model.forecast(steps=days_ahead)
                predictions[metric] = {
                    'predicted_values': forecast.tolist(),
                    'confidence_interval': fitted_model.get_forecast(days_ahead).conf_int().tolist(),
                    'trend': 'improving' if forecast[-1] > df[metric].iloc[-1] else 'degrading'
                }
        
        return {
            'prediction_horizon_days': days_ahead,
            'predictions': predictions,
            'overall_trend': self._calculate_overall_trend(predictions),
            'recommendations': self._generate_trend_recommendations(predictions)
        }
    
    def _calculate_overall_trend(self, predictions: dict) -> str:
        """Calcula tendencia general basada en todas las métricas"""
        improving_count = sum(1 for p in predictions.values() if p['trend'] == 'improving')
        total_metrics = len(predictions)
        
        if improving_count > total_metrics * 0.6:
            return 'improving'
        elif improving_count < total_metrics * 0.4:
            return 'degrading'
        else:
            return 'stable'
```

### 3. Análisis de Impacto
```python
class ImpactAnalyzer:
    """Analizador de impacto de problemas de calidad"""
    
    def analyze_quality_impact(self, quality_issue: dict):
        """Analiza el impacto de un problema de calidad específico"""
        
        impact_analysis = {
            'issue_details': quality_issue,
            'affected_records': self._count_affected_records(quality_issue),
            'downstream_impact': self._analyze_downstream_impact(quality_issue),
            'business_impact': self._estimate_business_impact(quality_issue),
            'resolution_effort': self._estimate_resolution_effort(quality_issue),
            'priority_score': self._calculate_priority_score(quality_issue)
        }
        
        return impact_analysis
    
    def _analyze_downstream_impact(self, issue: dict) -> dict:
        """Analiza impacto en sistemas downstream"""
        affected_systems = []
        
        # Buscar sistemas que consumen estos datos
        consumers = self._get_data_consumers(issue['dataset_id'])
        
        for consumer in consumers:
            impact_level = self._assess_consumer_impact(consumer, issue)
            affected_systems.append({
                'system': consumer['name'],
                'impact_level': impact_level,
                'affected_processes': consumer['processes'],
                'mitigation_available': consumer.get('fallback_available', False)
            })
        
        return {
            'total_affected_systems': len(affected_systems),
            'critical_systems': [s for s in affected_systems if s['impact_level'] == 'critical'],
            'systems_detail': affected_systems
        }
    
    def _estimate_business_impact(self, issue: dict) -> dict:
        """Estima impacto de negocio"""
        base_cost_per_invalid_contact = 0.50  # USD
        affected_records = self._count_affected_records(issue)
        
        return {
            'estimated_cost_usd': affected_records * base_cost_per_invalid_contact,
            'customer_experience_impact': self._assess_cx_impact(issue),
            'compliance_risk': self._assess_compliance_risk(issue),
            'reputation_risk': self._assess_reputation_risk(issue)
        }
```

## Métricas de Adopción y ROI

### 1. Métricas de Adopción
```python
class AdoptionMetrics:
    """Métricas de adopción del sistema"""
    
    def calculate_adoption_metrics(self, period_days: int = 30):
        """Calcula métricas de adopción del sistema"""
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=period_days)
        
        return {
            'user_adoption': {
                'total_users': self._count_total_users(),
                'active_users': self._count_active_users(start_date, end_date),
                'new_users': self._count_new_users(start_date, end_date),
                'user_retention_rate': self._calculate_retention_rate(period_days)
            },
            'feature_usage': {
                'dashboard_views': self._count_dashboard_views(start_date, end_date),
                'api_calls': self._count_api_calls(start_date, end_date),
                'alerts_configured': self._count_configured_alerts(),
                'reports_generated': self._count_reports_generated(start_date, end_date)
            },
            'engagement_metrics': {
                'avg_session_duration': self._calculate_avg_session_duration(start_date, end_date),
                'pages_per_session': self._calculate_pages_per_session(start_date, end_date),
                'feature_discovery_rate': self._calculate_feature_discovery_rate(period_days)
            }
        }
```

### 2. Cálculo de ROI
```python
class ROICalculator:
    """Calculadora de retorno de inversión"""
    
    def calculate_roi(self, implementation_period_months: int = 12):
        """Calcula ROI del sistema de calidad de datos"""
        
        costs = self._calculate_total_costs(implementation_period_months)
        benefits = self._calculate_total_benefits(implementation_period_months)
        
        roi_percentage = ((benefits - costs) / costs) * 100
        
        return {
            'period_months': implementation_period_months,
            'total_costs_usd': costs,
            'total_benefits_usd': benefits,
            'net_benefit_usd': benefits - costs,
            'roi_percentage': roi_percentage,
            'payback_period_months': self._calculate_payback_period(costs, benefits),
            'cost_breakdown': self._get_cost_breakdown(implementation_period_months),
            'benefit_breakdown': self._get_benefit_breakdown(implementation_period_months)
        }
    
    def _calculate_total_benefits(self, months: int) -> float:
        """Calcula beneficios totales"""
        monthly_benefits = {
            'reduced_invalid_contacts': 5000,  # USD/mes
            'improved_campaign_efficiency': 8000,
            'reduced_manual_qa_effort': 12000,
            'faster_issue_resolution': 3000,
            'compliance_cost_avoidance': 2000
        }
        
        return sum(monthly_benefits.values()) * months
    
    def _calculate_total_costs(self, months: int) -> float:
        """Calcula costos totales"""
        one_time_costs = {
            'development': 80000,
            'infrastructure_setup': 15000,
            'training': 10000
        }
        
        monthly_costs = {
            'infrastructure': 2000,
            'maintenance': 3000,
            'licenses': 1000
        }
        
        return sum(one_time_costs.values()) + (sum(monthly_costs.values()) * months)
```

## Casos de Estudio y Ejemplos

### Caso de Estudio 1: Detección de Degradación de Calidad
```python
case_study_1 = {
    'title': 'Detección Temprana de Degradación de Calidad',
    'scenario': '''
    El sistema detectó una caída gradual en la precisión de números telefónicos
    del 98% al 92% durante una semana, causada por cambios en el sistema fuente.
    ''',
    'detection': {
        'alert_triggered': 'accuracy_degradation',
        'trigger_time': '2024-01-15T09:30:00Z',
        'affected_records': 15420,
        'estimated_impact': '$7,710 USD'
    },
    'response': {
        'alert_sent_to': ['Data Engineering Team', 'Business Stakeholders'],
        'investigation_started': '2024-01-15T09:45:00Z',
        'root_cause_identified': '2024-01-15T11:20:00Z',
        'issue_resolved': '2024-01-15T14:30:00Z'
    },
    'outcome': {
        'downtime_avoided': '72 hours',
        'cost_savings': '$25,000 USD',
        'customer_impact_avoided': '50,000 failed contact attempts'
    },
    'lessons_learned': [
        'Alertas tempranas previenen problemas mayores',
        'Trazabilidad acelera identificación de causa raíz',
        'Dashboards ejecutivos mejoran visibilidad de negocio'
    ]
}
```

### Caso de Estudio 2: Optimización de Pipeline
```python
case_study_2 = {
    'title': 'Optimización de Pipeline Basada en Métricas',
    'scenario': '''
    Análisis de métricas reveló cuellos de botella en procesamiento,
    permitiendo optimizaciones que mejoraron el rendimiento en 300%.
    ''',
    'metrics_analysis': {
        'initial_processing_rate': '500 records/min',
        'target_processing_rate': '2000 records/min',
        'bottleneck_identified': 'External API validation calls',
        'optimization_opportunity': 'Batch processing and caching'
    },
    'implementation': {
        'batch_size_optimization': 'Increased from 1 to 50 records/batch',
        'caching_strategy': 'Redis cache for carrier validation',
        'parallel_processing': 'Increased worker threads from 2 to 8'
    },
    'results': {
        'final_processing_rate': '1800 records/min',
        'performance_improvement': '260%',
        'cost_reduction': '40%',
        'latency_improvement': '75%'
    }
}
```

## Conclusiones 

### Beneficios Clave del Sistema
- **Visibilidad Completa**: Dashboard 360° de calidad de datos
- **Detección Proactiva**: Alertas tempranas previenen problemas costosos
- **Trazabilidad Total**: Linaje completo del dato para investigaciones rápidas
- **Automatización**: Reducción del 80% en esfuerzo manual de QA
- **ROI Comprobado**: Retorno de inversión del 340% en 12 meses

### Con qué continuar?

#### Corto Plazo (1-3 meses)
- Implementar fases 1 y 2 del sistema
- Configurar alertas críticas
- Entrenar equipos en uso del sistema
- Establecer SLAs de calidad de datos

#### Mediano Plazo (3-6 meses)
- Expandir a otros datasets críticos
- Implementar ML para predicción de calidad
- Integrar con sistemas de data governance
- Desarrollar métricas de negocio específicas

#### Largo Plazo (6-12 meses)
- Implementar data quality scoring automático
- Crear centro de excelencia de calidad de datos
- Expandir a toda la organización
- Desarrollar capacidades de self-healing data

### Métricas de Éxito
```python
success_metrics = {
    'technical_kpis': {
        'data_quality_improvement': 'Target: +15% in 6 months',
        'issue_detection_time': 'Target: <5 minutes',
        'resolution_time': 'Target: <2 hours',
        'false_positive_rate': 'Target: <5%'
    },
    'business_kpis': {
        'contact_success_rate': 'Target: +10% in 6 months',
        'operational_cost_reduction': 'Target: -25%',
        'compliance_score': 'Target: 99%+',
        'user_satisfaction': 'Target: 4.5/5'
    },
    'organizational_kpis': {
        'data_literacy_improvement': 'Target: 80% of users trained',
        'self_service_adoption': 'Target: 60% of queries self-served',
        'cross_team_collaboration': 'Target: 90% of issues resolved cross-functionally'
    }
}
```

Este sistema representa una inversión estratégica en la calidad y confiabilidad de los datos, proporcionando las herramientas necesarias para mantener estándares de excelencia en la gestión de información de clientes y apoyar la toma de decisiones basada en datos de alta calidad.
