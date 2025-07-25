# MCP DB Schema Tools 🛠️

Claude Code용 MCP 서버로, JSON 기반 데이터베이스 스키마를 관리할 수 있습니다.

## ⚡ 빠른 설치 (uv 사용)

```bash
# uv 설치 (없다면)
curl -LsSf https://astral.sh/uv/install.sh | sh

# 프로젝트 설치
cd mcp-db-schema-tools
uv sync

# 설치 확인
uv run python -c "import mcp_db_schema_tools; print('MCP server module loaded successfully')"
```

## 🚀 Claude Code 설정

`.claude/settings.json`에 추가:

```json
{
  "mcpServers": {
    "db-schema-tools": {
      "command": "uv",
      "args": [
          "run",
          "--directory",
          "path/to/mcp-db-schema-tools",
          "python",
          "-m",
          "mcp_db_schema_tools"
      ]
    }
  }
}
```

## 🛠️ 사용 가능한 도구들

### 1. `json_to_sql`

JSON 스키마를 SQL DDL로 변환

```typescript
// Claude Code에서 사용
"JSON 스키마를 SQL로 변환해줘"
```

### 2. `sql_to_json`

기존 데이터베이스에서 JSON 스키마 추출

```typescript
"SQLite 파일에서 스키마를 JSON으로 추출해줘"
```

### 3. `merge_schemas`

여러 JSON 스키마 파일을 하나로 통합

```typescript
"core와 admin 스키마를 합쳐줘"
```

### 4. `validate_schema`

JSON 스키마의 무결성 검증

```typescript
"이 스키마에 문제없는지 검증해줘"
```

### 5. `create_database`

JSON 스키마로부터 실제 데이터베이스 생성

```typescript
"이 스키마로 SQLite 데이터베이스 만들어줘"
```

### 6. `compare_with_models` ✨ **NEW**

데이터베이스 스키마와 백엔드 모델 동기화 확인

```typescript
"현재 DB와 백엔드 모델이 동기화되어 있는지 확인해줘"
```

## 🎯 주요 특징

- ⚡ **uv 기반** - 초고속 의존성 관리
- 🔄 **양방향 변환** - JSON ↔ SQL 완벽 지원
- 🧩 **다중 스키마** - 모듈별 스키마 병합
- ✅ **자동 검증** - Foreign Key, 인덱스 무결성 확인
- 🎨 **Claude 통합** - 자연어로 스키마 관리
- 🚀 **MCP 1.12+** - 최신 MCP 프로토콜 지원
- 🔧 **자동 ID 생성** - 시드 데이터에서 누락된 Primary Key ID 자동 생성
- 🔍 **모델 동기화** - 백엔드 모델과 DB 스키마 동기화 상태 확인
- 📋 **향상된 ENUM** - ENUM 타입의 완벽한 JSON ↔ SQL 변환 지원

## 📋 지원하는 JSON 스키마 형식

```json
{
  "database": {
    "name": "my_app",
    "type": "sqlite",
    "version": "1.0.0"
  },
  "tables": {
    "users": {
      "description": "사용자 정보",
      "columns": {
        "id": {
          "type": "INTEGER",
          "primary_key": true,
          "auto_increment": true
        },
        "email": {
          "type": "VARCHAR(255)",
          "unique": true,
          "nullable": false
        }
      },
      "indexes": [
        {
          "name": "idx_users_email",
          "columns": ["email"],
          "unique": true
        }
      ]
    }
  },
  "relationships": [
    {
      "name": "posts_to_users",
      "from": "posts.user_id",
      "to": "users.id",
      "type": "many-to-one"
    }
  ]
}
```

## 🔧 개발

```bash
# 개발 환경 설정
uv sync --dev

# 테스트 실행
uv run pytest

# 코드 포맷팅
uv run black .
uv run ruff check .

# MCP 서버 실행 (디버그)
uv run python -m mcp_db_schema_tools
```

## 📦 배포

```bash
# 빌드
uv build

# 업로드 (PyPI)
uv publish
```

## 🎉 사용 예시

### Claude Code에서 자연어로 요청

```prompt
사용자: "blog_schema.json을 SQL로 변환해서 데이터베이스 만들어줘"

Claude: json_to_sql 도구를 사용해서 변환하겠습니다.
→ CREATE TABLE users (...) 
→ CREATE TABLE posts (...)
→ SQLite 데이터베이스 생성 완료!
```

```prompt
사용자: "기존 legacy.db에서 스키마를 추출해서 문서화해줘"

Claude: sql_to_json 도구로 스키마를 추출하겠습니다.
→ JSON 스키마 파일 생성
→ README.md에 테이블 관계도 생성
→ 문서화 완료!
```

```prompt
사용자: "현재 데이터베이스가 백엔드 모델과 동기화되어 있는지 확인해줘"

Claude: compare_with_models 도구로 확인하겠습니다.
→ ❌ Missing Tables: UserProfile, ActivityLog
→ ⚠️ Extra Tables: temp_migration_backup
→ 💡 Consider running migrations to sync with models
```

## 🤝 기여

1. Fork the repository
2. Create feature branch: `git checkout -b feature/amazing-feature`
3. Commit changes: `git commit -m 'Add amazing feature'`
4. Push to branch: `git push origin feature/amazing-feature`
5. Open Pull Request

## 📄 라이선스

MIT License - 자세한 내용은 [LICENSE](LICENSE) 파일을 참조하세요.

---

Made with ⚡ uv and ❤️ for Claude Code integration
