# دليل المساهمة في المشروع (Contributing Guide)

## قبل ما تبدأ

تأكد إنك:
1. عضو Collaborator في الريبو (لو مش متأكد كلم Ahmed)
2. عندك Docker Desktop شغال
3. عملت clone للمشروع:
```bash
git clone https://github.com/Ahmed-Elsom5raty/chicago-crime-project.git
cd chicago-crime-project
```

## خطوات الشغل اليومي

### 1. اسحب آخر تحديثات قبل ما تبدأ
```bash
git checkout main
git pull origin main
```

### 2. اعمل branch جديد لأي مهمة أو تعديل
ما تشتغلش على main مباشرة أبدًا.

```bash
git checkout -b feature/اسم-وصفي-للمهمة
```

أمثلة على أسماء branches:
- `feature/add-crime-dag`
- `fix/postgres-connection`
- `update/readme`

### 3. اعمل التعديلات بتاعتك وجربها محليًا
```bash
docker-compose up -d
```

### 4. لما تخلص، ارفع التعديل
```bash
git add .
git commit -m "وصف مختصر وواضح للتعديل"
git push origin feature/اسم-وصفي-للمهمة
```

### 5. افتح Pull Request على GitHub
- روح على صفحة الريبو
- هتلاقي زرار **Compare & pull request** ظاهر تلقائي
- اكتب وصف بسيط لإيه اللي عملته
- استنى مراجعة (review) من حد في التيم قبل الـ merge

### 6. بعد الموافقة (Approve)، اعمل Merge
دوس **Merge pull request** على GitHub.

### 7. امسح الـ branch بعد الدمج (اختياري بس نضافة)
```bash
git checkout main
git pull origin main
git branch -d feature/اسم-وصفي-للمهمة
```

## قواعد مهمة

- ❌ متعملش push مباشر على `main`
- ❌ متضيفش ملفات كبيرة زي `logs/` أو `.env` (موجودين في `.gitignore` بالفعل)
- ✅ اكتب commit messages واضحة (مش "update" أو "fix")
- ✅ جرب شغلك محليًا بـ `docker-compose up` قبل ما تعمل push
- ✅ لو فيه مشكلة أو تعارض (conflict)، كلم التيم قبل ما تحلها لوحدك

## لو حصل conflict عند الـ pull

```bash
git pull origin main
```
لو ظهرلك conflict، Git هيوريك في الملفات نفسها مكان المشكلة (بين `<<<<<<<` و `>>>>>>>`). افتح الملف، اختار السطور الصح، احذف العلامات دي، وبعدين:
```bash
git add .
git commit -m "Resolve merge conflict"
git push
```
