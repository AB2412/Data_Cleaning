# Creator:      Aleksey Bokow
# Updated by:   Sergii Butrymenko
# Dataset Name: Chicago Public Schools Suppliers
# Task #:       20
# Created:      April 2021
# Updated:      July 2021

# ruby mlc.rb --tool="clean::il::chicago_schools_payments" --start='names'
# ruby mlc.rb --tool="clean::il::chicago_schools_payments" --start='cities'
# ruby mlc.rb --tool="clean::il::chicago_schools_payments"

def execute(options = {})
  case options['start']
    when 'names'
    chicago_payments_cleaning_names
  when 'cities'
    chicago_payments_cleaning_cities
  else
    chicago_payments_cleaning_names
    chicago_payments_cleaning_cities
  end
end

def escape(str)
  str = str.to_s
  return str if str == ''

  str.gsub(/\\/, '\&\&').gsub(/'/, "''")
end

def get_source_oldest_date(db01)
  query = <<~SQL
    SELECT MIN(last_scrape_date) AS recent_date
    FROM chicago_public_schools_suppliers_payments;
  SQL
  db01.query(query).to_a.first['recent_date']
end

#Cleaning table_names

def chicago_payments_cleaning_names
  db01 = C::Mysql.on(DB01, 'usa_raw')

  begin
    query = <<~SQL
      SELECT MAX(scrape_date) AS recent_date
      FROM chicago_public_schools_suppliers_payments__name_clean;
    SQL
    recent_date = db01.query(query).to_a

  rescue Mysql2::Error

    create_table = <<~SQL
        CREATE TABLE chicago_public_schools_suppliers_payments__name_clean
        (id bigint(20) AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        clean_name VARCHAR(255),
        name_type VARCHAR(255),
        scrape_date DATE,
        fixed_manually BOOLEAN NOT NULL DEFAULT 0,
      CONSTRAINT unique_key Unique(name));
    SQL
    db01.query(create_table)

    message_to_slack 'Table *chicago_public_schools_suppliers_payments__name_clean* created'
    recent_date = []
  end

  recent_date = if recent_date.empty? || recent_date.first['recent_date'].nil?
                  get_source_oldest_date(db01) - 1
                else
                  recent_date.first['recent_date']
                end

  selecting = <<~SQL
    SELECT l.name, last_scrape_date
    FROM chicago_public_schools_suppliers_payments l
      LEFT JOIN chicago_public_schools_suppliers_payments__name_clean cl on l.name = cl.name
    WHERE cl.name IS NULL
      AND last_scrape_date >= '#{recent_date}'
    GROUP BY l.name;
  SQL
  selecting_new = db01.query(selecting)

  selecting_new.each_slice(1000).to_a.map do |each_set|
    inserting_new = 'INSERT IGNORE INTO chicago_public_schools_suppliers_payments__name_clean (name, scrape_date) VALUES '
    each_set.map do |row|
      inserting_new << "('#{escape(row['name'])}','#{row['last_scrape_date']}'),"
    end
    inserting_new.chop!
    db01.query(inserting_new)
  end

  names = <<~SQL
    SELECT DISTINCT
    name
    FROM chicago_public_schools_suppliers_payments__name_clean
    WHERE clean_name IS NULL;
  SQL
  new_names = db01.query(names)

  if new_names.size != 0
    det = MiniLokiC::Formatize::Determiner.new
    new_names.map do |row|
      n = row['name'].gsub(/\s{2,}(\d{1,2}|M)$/, '').squeeze(' ')
      type = det.determine(n)
      clean_name = if type == 'Person'
                     if n.include?(',')
                       MiniLokiC::Formatize::Cleaner.person_clean(n, true).gsub(/\d+/, '')
                     else
                       MiniLokiC::Formatize::Cleaner.person_clean(n, false).gsub(/\d+/, '')
                     end

                   else
                     MiniLokiC::Formatize::Cleaner.org_clean(n).sub(/^A&a /, 'A&A ').sub(/& a /, '& A ').gsub('. , ', '., ').gsub(/L.?L.?C.?/, 'LLC')
                   end.gsub(/"/, '').strip
      db01.query("UPDATE chicago_public_schools_suppliers_payments__name_clean SET clean_name = \"#{clean_name}\", name_type = '#{type}' WHERE name = '#{escape(row['name'])}' and clean_name IS NULL")
    end
  else
    message_to_slack 'There is no any new name to clean in *chicago_public_schools_suppliers_payments*'
  end
end

#Cleaning table_cities

def chicago_payments_cleaning_cities
  db01 = C::Mysql.on(DB01, 'usa_raw')

  begin
    query = <<~SQL
      SELECT MAX(scrape_date) AS recent_date
      FROM chicago_public_schools_suppliers_payments__city_clean;
    SQL
    recent_date = db01.query(query).to_a

  rescue Mysql2::Error

    creating_table = <<~SQL
      CREATE TABLE chicago_public_schools_suppliers_payments__city_clean
        (id bigint(20) AUTO_INCREMENT PRIMARY KEY,
        city VARCHAR(255) NOT NULL,
        clean_city VARCHAR(255) DEFAULT NULL,
        state VARCHAR(255) NOT NULL,
        scrape_date DATE,
        fixed_manually BOOLEAN NOT NULL DEFAULT 0,
        usa_adcp_matching_id BIGINT(20) DEFAULT NULL,
      CONSTRAINT state_city_idx Unique(state, city));
    SQL
    db01.query(creating_table)

    message_to_slack 'Table *chicago_public_schools_suppliers_payments__city_clean* created'
    recent_date = []
  end

  recent_date = if recent_date.empty? || recent_date.first['recent_date'].nil?
                  get_source_oldest_date(db01) - 1
                else
                  recent_date.first['recent_date']
                end

  select = <<~SQL
    SELECT l.city, l.state, last_scrape_date
    FROM chicago_public_schools_suppliers_payments l
      LEFT JOIN chicago_public_schools_suppliers_payments__city_clean cl on l.city = cl.city AND l.state = cl.state
    WHERE last_scrape_date >= '#{recent_date}'
      AND cl.city IS NULL
      AND cl.state IS NULL
    GROUP BY l.city, l.state;
  SQL
  select_new = db01.query(select)

  select_new.each_slice(1_000).to_a.map do |each_set|
    insert_new = 'INSERT IGNORE INTO chicago_public_schools_suppliers_payments__city_clean (city, state, scrape_date) VALUES '
    each_set.map do |row|
      insert_new << "('#{escape(row['city'])}', '#{row['state']}', '#{row['last_scrape_date']}'),"
    end
    insert_new.chop!
    db01.query(insert_new)
  end

  cities = <<~SQL
    SELECT city, state
    FROM chicago_public_schools_suppliers_payments__city_clean
    WHERE clean_city IS NULL;
  SQL
  new_cities = db01.query(cities).to_a

  if new_cities.empty?
    message_to_slack 'There is no any new city to clean in chicago_public_schools_suppliers_payments'
  else
    new_cities.map do |row|
      if row['city'].nil?
        city = ''
      else
        city = row['city'].gsub(/[,.]*$/, '')
        if city.downcase.end_with?('hgts') || (city.downcase.end_with?('hts') && !city.downcase.end_with?('heights'))
          city = city.sub(/hg?ts$/i, ' Heights')
        end
        city = city.gsub(/"/, '').gsub('/', ' ').gsub(/\b(HTS|HGTS|HGHTS)\b/i, 'Heights')
        city = city.gsub(/\bHLS\b/i, 'Hills')
        city = city.gsub(/\bSO\./i, 'South')
        city = city.gsub(/\b(VLG|VIL|VILLAG)\b/i, 'Village')
        city = city.gsub(/\bPK\b/i, 'Park').gsub(/\bEST\b/i, 'Estates')
        city = city.gsub(/Mt./i, 'Mount ')
        city = city.gsub(/\s+\(.*\)/, '')

      end
      clean_city = city.size >= 5 ? MiniLokiC::DataMatching::NearestWord.correct_city_name(city, row['state'],1) : nil
      clean_city = city.split(' ').map(&:capitalize).join(' ').strip if clean_city.nil?
      puts "#{row['city']} >>> #{clean_city}"
    end
    message_to_slack '*chicago_public_schools_suppliers_payments__city_clean* updated'
  end

  update_usa_adcp_matching_id(db01)
  db01.close
end

def update_usa_adcp_matching_id(route)
  query = <<~SQL
    SELECT DISTINCT clean_city, state, p.id AS usa_adcp_matching_id
    FROM chicago_public_schools_suppliers_payments__city_clean t
        JOIN hle_resources_readonly_sync.usa_administrative_division_states s ON t.state=s.short_name
        LEFT JOIN hle_resources_readonly_sync.usa_administrative_division_counties_places_matching p ON p.short_name=t.clean_city AND s.name=p.state_name
    WHERE state='IL'
      AND usa_adcp_matching_id IS NULL
      AND has_duplicate=0
      AND bad_matching IS NULL
      AND p.pl_production_org_id IS NOT NULL;
  SQL
  city_list = route.query(query)
  city_list.each do |item|
    query = <<~SQL
      UPDATE chicago_public_schools_suppliers_payments__city_clean
      SET usa_adcp_matching_id=#{item['usa_adcp_matching_id']}
      WHERE clean_city='#{escape(item['clean_city'])}'
        AND state = '#{item['state']}'
        AND usa_adcp_matching_id IS NULL;
    SQL
    puts query
    route.query(query)
  end
end

def message_to_slack(message)
  Slack::Web::Client.new.chat_postMessage(
      channel: 'UKLB1JGDN',
      text: "*[CLEANING #20] Chicago Public Schools Suppliers* \n>#{message}",
      as_user: true
  )
end