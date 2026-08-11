# Chicago Crime Data Engineering Pipeline

مشروع Data Engineering لمعالجة وتحليل بيانات الجرائم في شيكاغو، باستخدام Apache Airflow, Apache Spark, و Hadoop.

## المكونات (Services)

| Service | الوصف |
|---------|-------|
| **Airflow** | جدولة وتشغيل الـ pipelines (DAGs) |
| **Spark** | معالجة البيانات على نطاق واسع (Master + Worker) |
| **Hadoop (HDFS)** | تخزين البيانات الموزع (Namenode + Datanode) |
| **PostgreSQL** | قاعدة بيانات لتخزين النتائج / metadata بتاع Airflow |

## المتطلبات (Requirements)

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) مثبت وشغال
- Git

## طريقة التشغيل

1. اعمل clone للمشروع:
```bash
git clone https://github.com/Ahmed-Elsom5raty/chicago-crime-project.git
cd chicago-crime-project
```

2. شغل كل الـ containers:
```bash
docker-compose up -d
```

3. تأكد إن كل الـ containers شغالة:
```bash
docker-compose ps
```

## الوصول للخدمات (Access)

| Service | الرابط |
|---------|--------|
| Airflow Webserver | http://localhost:8080 |
| Spark Master UI | http://localhost:8080 (تأكد من البورت في docker-compose) |
| Hadoop Namenode UI | http://localhost:9000 |
| PostgreSQL | localhost:5432 |

## إضافة DAGs جديدة

حط ملفات الـ Python بتاعة الـ DAGs في فولدر `dags/`، وAirflow هيكتشفها تلقائي.

## إيقاف المشروع

```bash
docker-compose down
```

## ملاحظات

- ملف `.env` مش مرفوع على الريبو (موجود في `.gitignore`)، لو محتاج environment variables اعمل ملف `.env` بنفسك محليًا
- فولدر `logs/` مش مرفوع لتجنب تضخيم حجم الريبو
