"""Auth service"""

from msfwk.exceptions import DespGenericError
from msfwk.request import HttpClient
from msfwk.utils.logging import get_logger

from search.models.constants import EMAIL_NOT_OBTAINED

logger = get_logger("auth_service")


async def get_mail_from_desp_user_id(desp_user_id: str) -> str:
    """Get the mail from the desp user id

    Args:
        desp_user_id (str): id of the desp user

    Returns:
        str: mail of the desp user
    """

    def _raise_email_error(message: str, original_error: Exception | None = None) -> None:
        if original_error:
            raise DespGenericError(
                status_code=500,
                message=message,
                code=EMAIL_NOT_OBTAINED,
            ) from original_error
        raise DespGenericError(
            status_code=500,
            message=message,
            code=EMAIL_NOT_OBTAINED,
        )

    response_content = {}
    try:
        http_client = HttpClient()
        async with (
            http_client.get_service_session("auth") as http_session,
            http_session.get(f"/profile/{desp_user_id}") as response,
        ):
            if response.status != 200:  # noqa: PLR2004
                logger.error(await response.json())
                _raise_email_error(f"User {desp_user_id} not found")

            response_content = await response.json()
            email = response_content["data"].get("profile", {}).get("email")
            if not email:
                _raise_email_error(f"Email not defined for the user {desp_user_id}")
            logger.debug("Response from the auth service : %s", response_content)

            return email

    except Exception as E:
        msg = f"Exception while fetching the email from the auth service : {E}"
        logger.exception(msg)
        _raise_email_error(
            f"Could not call auth service to fetch the email of the user {desp_user_id} : {E}",
            E,
        )
