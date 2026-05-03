# GitHub Secrets を一括更新するスクリプト
# 使い方: PowerShell で実行 → ブラウザが各Secretsページを順に開く

$repo = "hannama-shitake/sp-api-jp"
$secrets = @{
    "AMAZON_JP_REFRESH_TOKEN" = "Atzr|IwEBIPb5E9UZCCwTJDnd2aLRI6qm97zuCAgo-VAMM0NQcq45NMG4DSo2-IvRr-_iyD5ZWoxbMq02ZFLDnuhqVVe7d1UDM2MvTk7rndWJw8pnUcuAsGAFcXpgw7BMnJ61DIOvZftmnRkuHOlzGApjLULRAi34ASA-kaYQ-EsEZJ3nkzk0YuxBJ11Az-KBuwByWCIEAkQTD5tpS8Ri2DJhBkuvdg3mM__0a1_NygZsA2QT0HOsFESTuPsgDuD_iaNG7nLX7v1v6z2omHlJ1pnCzhOAdwwUYeZY5iE78XSxIKJR-7v9O9qMpjS5nI8EW2b-8x50p4d0FHJlNXxK-ffzz-7g2nZ6"
    "AMAZON_JP_LWA_CLIENT_ID" = "amzn1.application-oa2-client.c084829d199f4612ae6bbef134c1536c"
    "AMAZON_JP_LWA_CLIENT_SECRET" = "amzn1.oa2-cs.v1.4b07e5491e5eacf4788a37cc73ae250945bd5e50af802c1bf1555697848088da"
    "AMAZON_AU_REFRESH_TOKEN" = "Atzr|IwEBIPXbTZV8WkW-wsLBcSKOaEMpm34W2WSe5MBPFrgO5dUGXqgbdi_Q2cshz9WryY82bpdqIYYTID2aoBt83Gcpb-Ogv2yYWt2WMINrKWoA56MaZcthd9G5sAiBiZ9UNapbc89RPP5a_BtNaYrhj9QfKfiV0kGwB2_C801A2j2rMsFIH4lO6kTRCeSU1tJalsImxk_p_jV4f0Ap8O5iHJsegrLKIRgNARqPrRGBWGF_3mSXTSTQQsfGBYavDMHG4e_SKLZY76u1FLMat1YUIvKxDDPTfzuCL-BMfXtGOhgeqGf6pNnbL1PjWvECmYW5zEpVUHo"
    "AMAZON_AU_LWA_CLIENT_ID" = "amzn1.application-oa2-client.78739d8e2333415785d122cd671a97ca"
    "AMAZON_AU_LWA_CLIENT_SECRET" = "amzn1.oa2-cs.v1.c339b2d959a93aa2c0a83005b102d9d80477c69bd71c0f2295f3b059f7081cf0"
}

foreach ($name in $secrets.Keys) {
    $value = $secrets[$name]
    # クリップボードにコピー
    Set-Clipboard -Value $value
    Write-Host "==================================="
    Write-Host "Secret: $name"
    Write-Host "値をクリップボードにコピーしました"
    Write-Host "ブラウザのテキストボックスに Ctrl+A → Ctrl+V で貼り付けてください"
    Write-Host ""
    # ブラウザで開く
    Start-Process "https://github.com/$repo/settings/secrets/actions/$name"
    Write-Host "Enterを押すと次のSecretに進みます..."
    Read-Host
}

Write-Host "全て完了しました！"
