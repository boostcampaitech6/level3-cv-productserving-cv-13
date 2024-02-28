from fastapi import APIRouter, HTTPException, status, UploadFile
from pydantic import BaseModel
from sqlmodel import Session
from PIL import Image
from torchvision.transforms import transforms

from database import PredictionResult, engine
from dependencies import get_model
from PIL import Image

router = APIRouter()


class PredictionResponse(BaseModel):
    id: int
    result: int


# FastAPI 경로
@router.post("/predict")
def predict(file: UploadFile) -> PredictionResponse:
    # TODO: 이미지 파일과 사이즈가 맞는지 검증
    try:
        image = Image.open(file.file)
        image.verify()
        image = Image.open(file.file)
    
    except Exception as e:
        raise HTTPException(detail="Input file is not a valid image", status_code=400)

    image_tensor = transforms.ToTensor()(image)

    if not image_tensor.shape == (3, 512, 384):
        raise HTTPException(detail="Input Image is not the correct size (3, 512, 384)", status_code=400)
    
    # TODO: 모델 추론
    model = get_model()
    prediction = int(model(image_tensor.unsqueeze(0)).argmax())

    # TODO: 결과를 데이터베이스에 저장 (PredictionResult 사용)
    result = PredictionResult(result=prediction)
    with Session(engine) as session:
        session.add(result)
        session.commit()
        session.refresh(result)

    # TODO: 응답하기
    return PredictionResponse(id=result.id, result=prediction)
