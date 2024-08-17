# Creator: Alex Kuzmenko

def cmte_cities_exchange_hash
  {'MT AIRY' => 'Mount Airy',
   'MT. GILEAD' => 'Mount Gilead',
   'CHAPEL' => 'Chapel Hill'}
end

def cmte_cities_to_skip
  ["ALEXANDER", "ALPHARETTA", "ARDEN", "ATLANTA", "BARIUM SPRINGS", "BEAR CREEK", "BOSTON", "CANDLER", "DRAPER", "ENKA",
   "ERIE", "LAWSONVILLE", "LOS ANGELES", "NC", "NEBO", "NEWELL", "NEW HILL", "PAW CREEK", "POWELLS POINT", "RTP", "SEMORA",
   "SILOAM", "SNOW CAMP", "STELLA", "SUGAR GROVE", "SUPPLY", "TYNER", "WEST END"]
end

def committee_cities_cleaning
  begin
    db13   = C::Mysql.on(DB13, 'nc_raw')
    db_pub = C::Mysql.on(DB01, 'hle_resources_readonly_sync')

    input_data = <<~SQL
      SELECT DISTINCT id, committee_city AS city
      FROM nc_campaign_committees
      WHERE id NOT IN (
        SELECT id FROM nc_campaign_committees
        WHERE (committee_city_cleaned IS NULL AND committee_city_manually_cleaned IS NOT NULL)
           OR (committee_city_cleaned IS NOT NULL AND committee_city_manually_cleaned IS NULL))
        AND committee_city IS NOT NULL
        AND committee_city != ''
        AND committee_state = 'NC';
    SQL
    data   = db13.query(input_data).to_a
    cities = data.map { |el| el['city'] }.uniq.sort

    cities.each do |cit|
      ids = data.select { |mel| mel['city'] == cit }.map { |el| el['id'] }.inject([]) { |arr, n| arr << n }.join(',')
      old_city = cit

      new_city = escape(easy_titleize(old_city.strip.gsub(/\t/, '')))
      match_found = true
      if cmte_cities_exchange_hash.has_key?(new_city.upcase)
        new_city = cmte_cities_exchange_hash[new_city.upcase]
        match_found = false
      else
        if pubs_matching_empty?(new_city, db_pub)
          next if cmte_cities_to_skip.include?(new_city.upcase)

          fix_try = try_fixing_city(old_city.multi_gsub!('  ').strip, state_list_cities(db_pub))
          new_city = fix_try if fix_try
          match_found = false unless fix_try
        end
      end

      ins_raw = match_found ? "SET committee_city_cleaned = #{new_city.dump}" : "SET committee_city_manually_cleaned = #{new_city.dump}"

      update_query = <<~SQL
        UPDATE nc_campaign_committees
        #{ins_raw}
        WHERE id IN (#{ids});
      SQL
      puts update_query
      db13.query(update_query)
    end
  rescue Mysql2::Error => e
    p e
  rescue Exception => e
    puts "!!\nException of SQL update algorithm\n!!"
    p e
  end
  db13.close if db13
  db_pub.close if db_pub
  puts 'Done'
end
