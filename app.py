import pandas as pd
import firebase_admin
from firebase_admin import firestore, credentials, auth

# 必要に応じて環境を変更
env = "dev"
# env = "prod"

cred = credentials.Certificate(f'./credentials/{env}.json')
app = firebase_admin.initialize_app(cred)
db = firestore.client()

def get_email_by_uid(uid):
    user = auth.get_user(uid)
    if user is None:
        return None
    return user.email

def get_uid_by_customer_id(customer_id):
    accountref = db.collection('account')
    docs = accountref.where('customer_number', '==', customer_id).stream()
    for doc in docs:
        return doc.id
    return None

def get_email(customer_id):
    uid = get_uid_by_customer_id(customer_id)
    if uid is None:
        return None
    return get_email_by_uid(uid)

def process():
    df = pd.read_csv('data/input.csv')
    df['email'] = df['会員コード'].apply(get_email)
    # 会員ID,会員コード,姓,名,電話番号,メールアドレス,ポイント,ポイント期限,最終来店日時,入会日,退会日,"案内メール受取許可フラグ (0:拒否, 1:許可)",備考,"会員状態区分 (0:利用可, 1:利用停止, 2:紛失, 3:退会, 4:名寄せ)",所属店舗,取引額,取引数,会員ランクコード,会員ランク名,メールアドレス2,メールアドレス3,会社名,部署名,役職,ポイント付与単位(金額),ポイント付与単位(ポイント),PINコード,社員ランクコード,社員ランク名,会員番号,旅券番号,国籍,アルファベット氏名,備考2,マイル
    df = df.reindex(columns=[
        '会員ID',
        '会員コード',
        '姓',
        '名',
        '電話番号',
        'メールアドレス',
        'email',
        'ポイント',
        'ポイント期限',
        '最終来店日時',
        '入会日',
        '退会日',
        '案内メール受取許可フラグ (0:拒否, 1:許可)',
        '備考',
        '会員状態区分 (0:利用可, 1:利用停止, 2:紛失, 3:退会, 4:名寄せ)',
        '所属店舗',
        '取引額',
        '取引数',
        '会員ランクコード',
        '会員ランク名',
        'メールアドレス2',
        'メールアドレス3',
        '会社名',
        '部署名',
        '役職',
        'ポイント付与単位(金額)',
        'ポイント付与単位(ポイント)',
        'PINコード',
        '社員ランクコード',
        '社員ランク名',
        '会員番号',
        '旅券番号',
        '国籍',
        'アルファベット氏名',
        '備考2',
        'マイル'
    ])
    df.to_csv('data/output.csv', index=False)

if __name__ == '__main__':
    process()
