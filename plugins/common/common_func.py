def get_sftp():
    print('sftp work start')

def regist(name, sex, **args):
    print(f'이름 : {name}')
    print(f'성별: {sex}')
    print(f'기타 옵션 : {args}')


def regist2(name, sex, *args, **kwargs):
    print(f'이름: {name}')
    print(f'성별: {sex}')
    print(f'기타옵션들: {args}')
    email = kwargs.get('email') or None
    phone = kwargs.get('phone') or None
    if email:
        print(email)
    if phone:
        print(phone)