# Creator: Alex Kuzmenko

def committee_cities_query
  <<~SQL
    SELECT DISTINCT
      state AS st,
      city
    FROM ny_campaign_finance_committees
    WHERE city_clean IS NULL
      AND state = 'NY'
    ORDER BY state, city;
  SQL
end

def committee_cities_cleaning
  begin
    db01 = C::Mysql.on(DB01, 'usa_raw')

    cities_to_clear = db01.query(committee_cities_query).to_a

    semaphore = Mutex.new
    threads = Array.new(10) do
      Thread.new do
        db01_ins = C::Mysql.on(DB01, 'usa_raw')
        db_pub   = C::Mysql.on(DB01, 'hle_resources_readonly_sync')

        loop do
          hash = nil
          semaphore.synchronize { hash = cities_to_clear.pop }

          break unless hash

          begin
            st       = hash['st']
            old_city = hash['city']

            new_city = make_city_full_cleaning(old_city)

            checked_data = db_pub.query(checking_existing_cities(new_city)).to_a

            next if checked_data.empty?

            update_query = <<~SQL
              UPDATE ny_campaign_finance_committees
              SET city_clean = #{new_city.dump}
              WHERE city = #{old_city.dump}
                AND state = #{st.dump};
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
        db_pub.close if db_pub
      end # thread
    end # threads
    threads.each(&:join)
  rescue Exception
    p "Something went wrong"
  end
  db01.close if db01
  puts 'Done'
end
