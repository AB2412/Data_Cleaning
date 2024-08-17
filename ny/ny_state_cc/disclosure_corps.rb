# Creator: Alex Kuzmenko

def disclosure_corps_query
  <<~SQL
    SELECT
      id,
      corp_30 AS corp
    FROM ny_campaign_finance_disclosure
    WHERE corp_name_clean IS NULL
      AND corp_30 != '';
  SQL
end

def disclosure_corps_cleaning
  begin
    db01 = C::Mysql.on(DB01, 'usa_raw')

    corps_to_clear = db01.query(disclosure_corps_query).to_a

    semaphore = Mutex.new
    threads = Array.new(10) do
      Thread.new do
        db01_ins = C::Mysql.on(DB01, 'usa_raw')

        loop do
          hash = nil
          semaphore.synchronize { hash = corps_to_clear.pop }

          break unless hash

          begin
            id       = hash['id']
            old_corp = hash['corp']
            new_corp = Cleaner.org_clean(old_corp)

            update_query = <<~SQL
              UPDATE ny_campaign_finance_disclosure
              SET corp_name_clean = #{new_corp.dump}
              WHERE id = #{id};
            SQL
            db01_ins.query(update_query)
          rescue Mysql2::Error => e
            p e
          rescue Exception => e
            puts "!!\nException of SQL update algorithm\n!!"
            p e
          end
        end # loop
        db01_ins.close if db01_ins
      end # thread
    end # threads
    threads.each(&:join)
  rescue Exception
    p "Something went wrong"
  end
  db01.close if db01
  puts 'Done'
end
