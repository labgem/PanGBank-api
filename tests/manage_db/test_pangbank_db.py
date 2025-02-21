# from typer.testing import CliRunner


# import pytest
# from sqlmodel import Session, SQLModel, create_engine
# from sqlmodel.pool import StaticPool

# from app.dependencies import get_session
# from app.main import app


# from app.manage_db.pangbank_db import cli

# runner = CliRunner()

# # @pytest.fixture(name="session")
# # def session_fixture():
# #     engine = create_engine(
# #         "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
# #     )
# #     SQLModel.metadata.create_all(engine)
# #     with Session(engine) as session:
# #         yield session


# # def test_app():
# #     result = runner.invoke(cli, ["Camila", "--city", "Berlin"])
# #     assert result.exit_code == 0
# #     assert "Hello Camila" in result.stdout
# #     assert "Let's have a coffee in Berlin" in result.stdout
