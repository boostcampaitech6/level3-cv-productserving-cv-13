# FastAPI Web Single Pattern
- 목적 : FastAPI를 사용해 Web Single 패턴을 구현합니다
- 상황 : 데이터 과학자가 model.py을 만들었고(model.joblib이 학습 결과), 그 model을 FastAPI을 사용해 Online Serving을 구현해야 함
  - model.py는 추후에 수정될 수 있으므로, model.py를 수정하지 않음(데이터 과학자쪽에서 수정)


# FastAPI 개발
1. 전체 구조를 생각 -> 파일, 폴더 구조를 어떻게 할까?
  - predict.py, api.py, config.py
  - 계층화 아키텍처
  - Presentation (API) <-> Application(Service) <-> Database
    API: 외부와 통신; 클라이언트에서 호출 가능
    - schema: 네트워크를 통해 데이터를 주고 받을 때 어떤 형태로 통신할지 정의하는 것
    - Request, Response 등등
    - Pydantic의 Basemodel 사용해서 정의 가능
    Application: 실제 로직; 예측 혹은 추론
    Database: 데이터 저장 및 가져오기

2. config -> DB -> Application -> API 역순으로 주로 개발 (마스터님의 경우)


# 구현해야 하는 기능
- TODO Tree 확장 프로그램도 사용 가능

- [] FastAPI 서버 만들기
- [] POST /predict : 예측 진행 후 PredictResponse 반환
- [] PredictResponse 저장 (DB에 저장)
- [] GET /predict : DB에 저장된 모든 PredictResponse 반환
- [] GET /predict/{id} : 해당 id의 Predictesponse 반환
- [] Lifespan; fastapi 띄워질 때 모델 로딩
- [] Config 설정
- [] DB 객체 만들기

# SQLModel
- Python ORM(Object Relational Mapping) : 객체 -> DB
- Session: 데이터베이스의 연결을 관리하는 방식
  - 음식점에 가서 입장, 주문, 식사, 퇴장까지를 하나의 Session으로 표현
  - Session 내에서 POST, GET, PATCH 등으로 데이터 추가/조회/수정 가능
- Transaction: 트랜잭션 완료 시 결과가 DB에 저장

```
SQLModel.metadata.create_all(engine)
- 처음에 init할 때 테이블 생성하는 코드
```

```
with Session(engine) as session:
  result = ''
  session.add(result) # 새로운 객체를 세션에 추가; 아직 DB에 저장 X
  session.commit() # 세션의 변경 사항을 DB에 저장
  session.refresh(result) # 세션에 있는 객체 업데이트

  session.get(DB Model, id) # id에 맞는 값 리턴
  session.query(DB Model).all() # 쿼리를 써서 모든 값을 가져오겠다
```

# SQLite3
- 가볍게 사용할 수 있는 데이터베이스(배포용은 아님)
