# Creator: Alex Kuzmenko

def cntrb_cities_exchange_hash
  {'SIMPSONVILLE' => 'Simpson',
   'WILLISTON' => 'Williston',
   'HOLDEN' => 'Holden Beach'
  }
end

def cntrb_cities_to_skip
  ["ARDEB", "ARDEN", "ARLINGTON", "BARCO", "BEAR CREEK", "BELES CREEK", "BOOMER", "BOSTON", "BURP", "CHARM", "CRESTON", "CROFTON",
   "GAFNEE", "GATES", "GOLDEN", "HAMPTON", "HUSK", "JARVISBURG", "JAX", "LAKELAND", "LAWSONVILLE", "LOTTY HAWK", "LOWLAND", "MAPLE",
   "NEW", "NEW HILL", "NEWELL", "NONE", "OTWAY", "PINE HALL", "PINEY CREEK", "R", "ROAD", "ROCKHILL", "ROCK HILL", "SCRANTON",
   "SHERMAN", "SILSAN", "STANDFORD", "STELLA", "TOPTON", "VASHON", "WARENSVILLE", "WRIGHTSVILLE", "X"]
end

def contributor_cities_cleaning
  begin
    db13 = C::Mysql.on(DB13, 'nc_raw')

    input_data = <<~SQL
      SELECT DISTINCT id, contributor_city AS city
      FROM nc_campaign_contributions
      WHERE id NOT IN (
        SELECT id FROM nc_campaign_contributions
        WHERE (contributor_city_cln_matched IS NULL AND contributor_city_clean IS NOT NULL)
           OR (contributor_city_cln_matched IS NOT NULL AND contributor_city_clean IS NULL))
        AND contributor_city IS NOT NULL
        AND contributor_city != ''
        AND contributor_state = 'NC';
    SQL
    data   = db13.query(input_data).to_a
    cities = data.map { |el| el['city'] }.uniq.sort

    semaphore = Mutex.new
    threads = Array.new(5) do
      Thread.new do
        db13_ins   = C::Mysql.on(DB13, 'nc_raw')
        db_pub_ins = C::Mysql.on(DB01, 'hle_resources_readonly_sync')

        loop do
          cit = nil
          semaphore.synchronize { cit = cities.pop }

          break unless cit

          ids = data.select { |mel| mel['city'] == cit }.map { |el| el['id'] }.inject([]) { |arr, n| arr << n }.join(',')
          old_city = cit

          new_city = escape(easy_titleize(old_city.strip.gsub(/\t/, '')))
          match_found = true
          if cntrb_cities_exchange_hash.has_key?(new_city.upcase)
            new_city = cntrb_cities_exchange_hash[new_city.upcase]
            match_found = false
          else
            if pubs_matching_empty?(new_city, db_pub_ins)
              unless cntrb_cities_to_skip.include?(new_city.upcase)
                fix_try = try_fixing_city(old_city.multi_gsub!('  ').strip, state_list_cities(db_pub_ins))
              else
                fix_try = nil
              end

              new_city = fix_try if fix_try
              match_found = false unless fix_try
            end
          end

          ins_raw = match_found ? "SET contributor_city_cln_matched = #{new_city.dump}" : "SET contributor_city_clean = #{new_city.dump}"

          update_query = <<~SQL
            UPDATE nc_campaign_contributions
            #{ins_raw}
            WHERE id IN (#{ids});
          SQL
          db13_ins.query(update_query)
        end # loop
        db13_ins.close if db13_ins
        db_pub_ins.close if db_pub_ins
      end # Thread
    end # Threads
    threads.each(&:join)
  rescue Mysql2::Error => e
    p e
  rescue Exception => e
    puts "!!\nException of SQL update algorithm\n!!"
    p e
  end
  db13.close if db13
  puts 'Done'
end
