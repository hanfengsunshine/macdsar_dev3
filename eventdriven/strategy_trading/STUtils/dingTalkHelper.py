import requests
import json
import aiohttp


class DingTalk_Base:
    def __init__(self):
        self.__headers = {'Content-Type': 'application/json;charset=utf-8'}
        self.url = ''

    def send_msg(self, text):
        json_text = {
            "msgtype": "text",
            "text": {
                "content": text
            },
            "at": {
                "atMobiles": [
                    ""
                ],
                "isAtAll": False
            }
        }
        return requests.post(self.url, json.dumps(json_text), headers=self.__headers).content

    async def async_send(self, text):
        await self.async_send_more(text, [""], False)

    async def async_send_more(self, text, phone_nums, isAtAll=False):
        json_text = {
            "msgtype": "text",
            "text": {
                "content": text
            },
            "at": {
                "atMobiles": phone_nums,
                "isAtAll": isAtAll
            }
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(self.url, json=json_text, headers=self.__headers) as resp:
                if resp.status == 200:
                    pass
                else:
                    raise ValueError(resp.status)


class DingTalk_Disaster(DingTalk_Base):
    def __init__(self, access_token):
        super().__init__()
        self.url = 'https://oapi.dingtalk.com/robot/send?access_token={}'.format(access_token)



