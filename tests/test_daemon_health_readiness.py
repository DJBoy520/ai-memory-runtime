from unittest.mock import MagicMock
import pytest
from src.main import AMRApplication


@pytest.mark.asyncio
async def test_system_health_reports_sqlite_read_failure():
    store = MagicMock()
    store.get_connection.return_value.execute.side_effect = RuntimeError('disk unavailable')
    engine = MagicMock(); engine.state.value = 'READY'
    qdrant = MagicMock(); qdrant.is_healthy.return_value = True
    app = AMRApplication(engine=engine,qdrant_manager=qdrant,session_store=store)
    result = await app.dispatch_business_rpc('system.health',{'agent_id':'system'})
    assert result['sqlite_ok'] is False
