# Creator: Alex Kuzmenko

def committee_names_query
  <<~SQL
    SELECT
      id,
      treas_first_name AS fn,
      treas_last_name AS ln
    FROM ny_campaign_finance_committees
    WHERE treas_full_name IS NULL;
  SQL
end

def committee_names_cleaning
  begin
    db01 = C::Mysql.on(DB01, 'usa_raw')

    names_to_clear = db01.query(committee_names_query).to_a

    semaphore = Mutex.new
    threads = Array.new(10) do
      Thread.new do
        db01_ins = C::Mysql.on(DB01, 'usa_raw')

        loop do
          hash = nil
          semaphore.synchronize { hash = names_to_clear.pop }

          break unless hash

          begin
            id     = hash['id']
            new_ln = hash['ln'] ? F::Cleaner.person_clean(hash['ln'].gsub(/^[^a-zA-Z]+/, ''), reverse = false) : ''
            new_fn = hash['fn'] ? F::Cleaner.person_clean(hash['fn'].gsub(/^[^a-zA-Z]+/, ''), reverse = false) : ''
            new_full_name = "#{new_fn} #{new_ln}".strip

            next if new_full_name.empty? || new_full_name.index(/^[^a-zA-Z]+/)

            update_query = <<~SQL
              UPDATE ny_campaign_finance_committees
              SET treas_full_name = #{new_full_name.dump}
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
