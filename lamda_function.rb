require "json"
require "net/http"
require 'base64'
require "date"
require 'time'
require "csv"
require 'aws-sdk-s3' 

# APIキーでAPIを呼び出す
def call_charges(params)
  # 実際にはenvから読むのを推奨
  api_key = ENV["payjp_private_key"]
  payjp_api = 'https://api.pay.jp/v1/charges'
  uri = URI.parse(payjp_api)
  uri.query = URI.encode_www_form(params)
  request = Net::HTTP::Get.new(uri.request_uri)
  request["Authorization"] = "Basic #{Base64.strict_encode64("#{api_key}:")}"
  request["Content-Type"]  = 'applicaction/x-www-form-url'
  http = Net::HTTP.new(uri.host, uri.port)
  http.use_ssl = uri.scheme === "https"

  response = http.request(request)
  result = JSON.parse(response.body)
  if response.code != "200"
    # リクエストエラーなのでそのままログに出しちゃう
    puts response.code, result
    return false
  else
    return result
  end

end

def lambda_handler(event:, context:)
  # リクエストパラメータ
  params = Hash.new
  params.store("limit", 100)
  params.store("offset","0")
  # 月初から月末まで
  #params.store("until", Date.new(Date.today.year, Date.today.month, -1).to_time.to_i)
  #params.store("since", Date.new(Date.today.year, Date.today.month,  1).to_time.to_i)
  result = call_charges(params)
  if !result
    return
  end

  # CSV書き込み開始
  key = Date.today.strftime("%Y%m%d") + ".csv"
  csv_path = "/tmp/" + key 
  csv = CSV.open(csv_path, "wb")
  csv.puts ["作成日", "定期課金ID", "顧客ID",'決済ステータス(成功or失敗)', "金額"]

  total = 0
  # 次のページがあるだけ繰り返す
  has_more = true
  while has_more do
    total = total + result["count"].to_i
    result["data"].each do |data|
      # CSVファイルに書き込む内容を生成する
      csv.puts [
        Time.at(data["created"]).strftime("%Y/%m/%d %H:%M:%S"),
        data["subscription"],
        data["customer"],
        data["paid"],
        data["amount"]
      ]
    end
    # offsetを変更してリクエストする
    params.store("offset", total)
    result = call_charges(params)
    if !result
      csv.close
      return
    end
    has_more = result["has_more"]
  end
  csv.close

  # S3に書き込む
  bucket_name = 'payjp-data'
  object_key = key
  region = 'ap-northeast-1'
  s3 = Aws::S3::Object.new(bucket_name, key, region: region)
  s3.upload_file(csv_path)


  { statusCode: 200, body: JSON.generate(total) }
end

