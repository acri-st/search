"""Models for the mails to send to the users"""

from msfwk.models import BaseModelAdjusted

""" Example call to send a mail to a user
await send_email_to_mq(
            notification_type=NotificationTemplate.GENERIC,
            user_email=user.profile.email,
            subject="New asset matches your subscription",
            message=f"New asset matches your subscription: {asset.name}",
            user_id=user.id
        )
"""


class SubscriptionMatchMail(BaseModelAdjusted):
    """A mail to send to a user about a subscription match"""

    user_email: str
    subject: str
    message: str
    user_id: str
