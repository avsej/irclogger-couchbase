require 'rubygems'
require 'bundler/setup'
Bundler.require(:default)
require 'date'
require 'uri'
require 'digest/md5'

client = Couchbase.new(ENV["BUCKET_URI"] || "http://localhost:8091/pools/default")
client.flush

to = from = Time.now.to_i
epoch = 1199145600  # 2008-01-01
week = 604800
channel = "#redis".gsub(/#/, '.')
total = 0
silence = 0

client.async = true

while to > epoch
  processed = 0
  to = from - 1
  from = to - week
  dl_last = 0

  url = "http://irclogger.com/#{channel}/slice/#{from}/#{to}"
  curl = Curl::Easy.new(url)
# curl.on_progress do |dl_total, dl_now, ul_total, ul_now|
#   if dl_now - dl_last
#     # fetch on_body callback
#     on_body = curl.on_body; curl.on_body(&ob_body)
#     on_body.call(curl.body_str[dl_prev...dl_now])
#   end
#   dl_last = dl_now
#   true
# end
  YAJI::Parser.new(curl, :check_utf8 => false).each("/") do |msg|
    # fix permalink
    msg["permalink"] = msg["permalink"].gsub('#', '.').gsub(/\.(?:msg_)?(\d+)$/, '#\1')
    msg.delete("id")
    client.set(Digest::SHA1.hexdigest(msg["permalink"]), msg)
    processed += 1
    total += 1
    print "\r#{url} ... #{processed} (total: #{total})"
    STDOUT.flush
    if total % 100 == 0
      client.run
    end
  end
  client.run
  puts "\r#{url} ... #{processed}                            "
  STDOUT.flush
  silence += 1 if processed == 0
  exit if silence == 3
end
