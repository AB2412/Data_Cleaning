# Creator:      Sergii Butrymenko
# Dataset Name: California State Campaign Finance
# Task #:       17
# Created:      April 2021

# ruby mlc.rb --tool="clean::ca::ca_state_cc_clean" --mode='committees'
# ruby mlc.rb --tool="clean::ca::ca_state_cc_clean" --mode='candidates'
# ruby mlc.rb --tool="clean::ca::ca_state_cc_clean" --mode='contributors'
# ruby mlc.rb --tool="clean::ca::ca_state_cc_clean" --mode='payees'
# ruby mlc.rb --tool="clean::ca::ca_state_cc_clean" --mode='all'
# 00 12 * * 5 /bin/bash mini_lokic_individual.sh --tool="clean::ca::ca_state_cc_clean" --mode='all' 2>&1 /home/loki/mini_lokic_log/tools/clean/ca/ca_state_cc_clean/ca_state_cc_clean

def execute(options = {})
  route = C::Mysql.on(DB13, 'ca_raw')
  case options['mode']
  when 'committees'
    # committees_cleaning_old(route)
    committees_cleaning(route)
    # committee_cities_cleaning(route)
    check_unmatched(route)
  when 'candidates'
    candidates_cleaning(route)
  when 'contributors'
    contributors_cleaning(route)
  when 'payees'
    payees_cleaning(route)
  when 'all'
    committees_cleaning(route)
    # committee_cities_cleaning(route)
    check_unmatched(route)

    candidates_cleaning(route)
    contributors_cleaning(route)
    payees_cleaning(route)
  else
    nil
  end
  route.close
end

def message_to_slack(message, type = '')
  type = case type
         when :alert
           ':error:'
         when :warning
           ':warning:'
         when :info
           ':information_source:'
         else
           ''
         end
  Slack::Web::Client.new.chat_postMessage(
    channel: 'UKLB1JGDN',
    text: "*[CLEANING #17] California State Campaign Finance* \n>#{type} #{message}",
    as_user: true
  )
end

def escape(str)
  str = str.to_s
  return str if str.nil? || str.empty?

  str.gsub(/\\/, '\&\&').gsub(/'/, "''")
end

def get_prepared_name(name)
  while (name.start_with?('"') && name.end_with?('"')) || name.start_with?("'") && name.end_with?("'")
    name = name[1..-1].strip
  end
  name.sub(/^" /, '"').gsub(' ",', '",')
end

def get_clean_name(name, type=:both)
  case type
  when :org
    name = MiniLokiC::Formatize::Cleaner.org_clean(name)
  when :ind
    name = MiniLokiC::Formatize::Cleaner.person_clean(name)
  else
    det = MiniLokiC::Formatize::Determiner.new
    name = det == 'Person' ? MiniLokiC::Formatize::Cleaner.person_clean(name) : MiniLokiC::Formatize::Cleaner.org_clean(name)
  end

  case name.count('"')
  when 1
    name = name.sub('"', '')
  when 2
    name = name.sub('", "', ', ')
  else
    nil
  end
  name.sub(/^"a /, '"A ').sub(/^THE /, 'The ').sub(/^A&a /, 'A&A ').sub(/& a /, '& A ').gsub('. , ', '., ').gsub(', , ', ', ').gsub(' L.L.C.', ' LLC')
end

# def committees_cleaning_old(route)
#   query = <<~SQL
#     SELECT c.committee_name, DATE(updated_at) AS created_at
#     FROM ca_campaign_finance_committees c
#              LEFT JOIN ca_campaign_finance_committees__cleaned n ON c.committee_name = n.committee_name
#     WHERE DATE(updated_at) >= IFNULL((SELECT MAX(scrape_date)
#                                 FROM ca_campaign_finance_committees__cleaned), '2021-04-01')
#       AND n.committee_name IS NULL
#       AND c.committee_name IS NOT NULL
#     GROUP BY c.committee_name;
#   SQL
#   puts query.green
#   new_data = route.query(query).to_a
#   if new_data.size > 0
#     new_data = new_data.each_slice(20_000).to_a
#     new_data.each do |var|
#       insert = "INSERT IGNORE INTO ca_campaign_finance_committees__cleaned (committee_name, committee_name_clean, scrape_date) VALUES "
#       var.each do |v|
#         name_clean = get_prepared_name(v['committee_name'])
#         name_clean = get_clean_name(name_clean, :org)
#         insert << "('#{escape(v['committee_name'])}', '#{escape(name_clean)}', '#{v['created_at']}'),"
#       end
#       puts "#{insert.chop};"
#       route.query("#{insert.chop};")
#     end
#   end
# end

def committees_cleaning(route)
  query = <<~SQL
    SELECT cmte.*,
           adrs.city, adrs.state AS state_code, LPAD(LEFT(adrs.zip, 5), 5, '0') AS zip5, adrs.address_updated_at
    FROM
        (
            SELECT DISTINCT committee_id, committee_name, DATE(MIN(updated_at)) AS scrape_date
            FROM ca_campaign_finance_committees
            GROUP BY committee_id, committee_name
        ) cmte
            JOIN
        (
            SELECT a1.*
            FROM ca_campaign_finance_committees__addresses a1
                     INNER JOIN (SELECT committee_id, MAX(address_updated_at) AS max_adr_updated_at
                                 FROM ca_campaign_finance_committees__addresses
                                 GROUP BY committee_id) a2 ON a1.committee_id=a2.committee_id AND a1.address_updated_at=a2.max_adr_updated_at
        ) adrs ON cmte.committee_id=adrs.committee_id
    LEFT JOIN ca_campaign_finance_committees__matched m ON adrs.committee_id=m.committee_id AND adrs.address_updated_at=m.address_updated_at
    WHERE m.id IS NULL;
  SQL
  puts query.green
  new_data = route.query(query, symbolize_keys: true).to_a
  if new_data.size > 0
    new_data = new_data.each_slice(20_000).to_a
    new_data.each do |row|
      insert = "INSERT IGNORE INTO ca_campaign_finance_committees__matched (committee_id, committee_name, committee_name_clean, city, city_clean, state_code, zip5, scrape_date, address_updated_at) VALUES "
      row.each do |item|
        name_clean = get_prepared_name(item[:committee_name])
        name_clean = get_clean_name(name_clean, :org)
        insert << "('#{item[:committee_id]}', '#{escape(item[:committee_name])}', '#{escape(name_clean)}', '#{escape(item[:city])}', '#{escape(get_clean_city_name(item[:city], item[:state_code]))}', '#{item[:state_code]}', '#{item[:zip5]}', '#{item[:scrape_date]}', '#{item[:address_updated_at]}'),"
      end
      # puts "#{insert.chop};"
      route.query("#{insert.chop};")
    end
  end
end

def get_clean_city_name(city, state)
  city_name = city.dup
  city_name = city_name.split(',')[0].split(/\b/).map(&:capitalize).join.sub("'S", "'s") unless city_name.empty?
  correct_city_name = nil
  correct_city_name = MiniLokiC::DataMatching::NearestWord.correct_city_name(city_name, state, 1) if city_name.length > 5
  if correct_city_name.nil?
    MiniLokiC::DataMatching::NearestWord.shortening_fix(MiniLokiC::DataMatching::NearestWord.lstrip_nonalpha(city_name))
  else
    correct_city_name
  end
end

def check_unmatched(route)
  query = <<~SQL
    SELECT COUNT(*) AS total_unmatched
    FROM ca_campaign_finance_committees__matched
    WHERE pl_production_org_id IS NULL
      AND state_code = 'CA'
      AND committee_id IN
        (SELECT DISTINCT committee_id
         FROM ca_campaign_finance_contributions
         UNION
         SELECT DISTINCT expender_id
         FROM ca_campaign_finance_expenditures);
  SQL
  total_unmatched = route.query(query).to_a.first['total_unmatched']
  if total_unmatched.zero?
    message_to_slack('There is no unmatched committees in *ca_campaign_finance_committees__matched* table.')
  else
    message_to_slack("#{total_unmatched} committees in *ca_campaign_finance_committees__matched* table should be matched.", :warning)
  end
end

# def committee_cities_cleaning(route)
#   attempt = 1
#   cleaned = false
#   query = <<~SQL
#     SELECT id, TRIM(city) AS city, state, TRIM(zip) AS zip
#     FROM ca_campaign_finance_committees__addresses
#     WHERE city IS NOT NULL
#       AND city<>''
#       AND city_clean IS NULL
#     LIMIT 10000;
#   SQL
#   begin
#     check_query = <<~SQL
#       SELECT city_clean, zip5, city_org_id
#       FROM ca_campaign_finance_committees__addresses
#       LIMIT 1;
#     SQL
#     puts query.green
#     route.query(check_query)
#   rescue Mysql2::Error
#     if attempt > 1
#       message_to_slack("Can't add columns to *ca_campaign_finance_committees__addresses* table. Check manually. Exiting...", :alert)
#       return
#     end
#     message_to_slack("Clean columns don't exist in *ca_campaign_finance_committees__addresses* table. Creating them now...", :warning)
#     add_query = <<~SQL
#       ALTER TABLE ca_campaign_finance_committees__addresses
#         ADD COLUMN city_clean VARCHAR(255) DEFAULT NULL AFTER city,
#         ADD COLUMN zip5 VARCHAR(5) DEFAULT NULL AFTER zip,
#         ADD COLUMN city_org_id INT DEFAULT NULL AFTER zip5;
#     SQL
#     puts add_query.red
#     route.query(add_query)
#     attempt += 1
#     retry
#   end
#   until cleaned
#     city_list = route.query(query, symbolize_keys: true).to_a
#     break if city_list.size == 0
#     city_list.each do |item|
#       city_name = item[:city]
#       city_name = city_name.split(',')[0].split(/\b/).map(&:capitalize).join.sub("'S", "'s") unless city_name.empty?
#       correct_city_name = MiniLokiC::DataMatching::NearestWord.correct_city_name(city_name, item[:state], 1) if city_name.length > 5
#       city_name =
#         if correct_city_name.nil?
#           MiniLokiC::DataMatching::NearestWord.shortening_fix(MiniLokiC::DataMatching::NearestWord.lstrip_nonalpha(city_name.dup)).sub(/\bSPG\b/i, 'Springs')
#         else
#           correct_city_name
#         end
#       zip5 =
#         case
#         when item[:zip].match(/^\d{4}$/)
#           "0#{item[:zip]}"
#         when item[:zip].match(/^\d{5}/)
#           item[:zip][0..4]
#         else
#           ''
#         end
#       city_org_id = get_city_org_id(item[:state], city_name, route)
#       update_query = <<~SQL
#        UPDATE ca_campaign_finance_committees__addresses
#        SET city_clean = '#{escape(city_name)}',
#            zip5 = '#{zip5}',
#            city_org_id = #{city_org_id}
#        WHERE id=#{item[:id]};
#       SQL
#       # puts update_query
#       route.query(update_query)
#     end
#   end
# end

def get_city_org_id(state_code, city, route)
  query = <<~SQL
    SELECT id, short_name, pl_production_org_id
    FROM hle_resources_readonly_sync.usa_administrative_division_counties_places_matching
    WHERE state_name=(SELECT name
                    FROM hle_resources_readonly_sync.usa_administrative_division_states
                    WHERE short_name='#{state_code}')
      AND short_name='#{escape(city)}'
      AND bad_matching IS NULL
      AND has_duplicate=0
      AND pl_production_org_id IS NOT NULL;
  SQL
  # puts query.green
  res = route.query(query).to_a
  if res.empty? || res.count > 1
    'NULL'
  else
    res.first['pl_production_org_id']
  end
end

def candidates_cleaning(route)
  query = <<~SQL
    SELECT c.full_name, DATE(updated_at) AS created_at
    FROM ca_campaign_finance_candidates c
             LEFT JOIN ca_campaign_finance_candidates__cleaned n ON c.full_name = n.name
    WHERE DATE(updated_at) >= IFNULL((SELECT MAX(scrape_date)
                                FROM ca_campaign_finance_candidates__cleaned), '2021-04-01')
      AND n.name IS NULL
      AND c.full_name IS NOT NULL
    GROUP BY c.full_name;
  SQL
  puts query.green
  new_data = route.query(query).to_a
  if new_data.size > 0
    new_data = new_data.each_slice(20_000).to_a
    new_data.each do |var|
      insert = "INSERT IGNORE INTO ca_campaign_finance_candidates__cleaned (name, name_clean, scrape_date) VALUES "
      var.each do |v|
        name_clean = get_prepared_name(v['full_name'])
        name_clean = get_clean_name(name_clean, :ind)
        insert << "('#{escape(v['full_name'])}', '#{escape(name_clean)}', '#{v['created_at']}'),"
      end
      puts "#{insert.chop};"
      route.query("#{insert.chop};")
    end
  end
end

def contributors_cleaning(route)
  query = <<~SQL
    SELECT contributor_name, created_at
    FROM ca_campaign_finance_contributions c
             LEFT JOIN ca_campaign_finance_contributor_names__cleaned n ON contributor_name = n.name
    WHERE created_at >= IFNULL((SELECT MAX(scrape_date)
                                FROM ca_campaign_finance_contributor_names__cleaned), '2021-04-01')
      AND n.name IS NULL
      AND contributor_name IS NOT NULL
    GROUP BY contributor_name;
  SQL
  puts query.green
  new_data = route.query(query).to_a
  if new_data.size > 0
    new_data = new_data.each_slice(20_000).to_a
    det = MiniLokiC::Formatize::Determiner.new
    new_data.each do |var|
      insert = "INSERT IGNORE INTO ca_campaign_finance_contributor_names__cleaned (name, name_clean, name_type, scrape_date) VALUES "
      var.each do |v|
        name_clean = get_prepared_name(v['contributor_name'])
        name_type = det.determine(name_clean)
        name_clean = get_clean_name(name_clean, name_type == 'Person' ? :ind : :org)
        insert << "('#{escape(v['contributor_name'])}', '#{escape(name_clean)}', '#{name_type}', '#{v['created_at']}'),"
      end
      puts "#{insert.chop};"
      route.query("#{insert.chop};")
    end
  end
end

def payees_cleaning(route)
  query = <<~SQL
    SELECT expender, created_at
    FROM ca_campaign_finance_expenditures c
             LEFT JOIN ca_campaign_finance_expender_names__cleaned n ON expender = n.name
    WHERE created_at >= IFNULL((SELECT MAX(scrape_date)
                                FROM ca_campaign_finance_expender_names__cleaned), '2021-04-01')
      AND n.name IS NULL
      AND expender IS NOT NULL
    GROUP BY expender;
  SQL
  puts query.green
  new_data = route.query(query).to_a
  if new_data.size > 0
    new_data = new_data.each_slice(20_000).to_a
    det = MiniLokiC::Formatize::Determiner.new
    new_data.each do |var|
      insert = "INSERT IGNORE INTO ca_campaign_finance_expender_names__cleaned (name, name_clean, name_type, scrape_date) VALUES "
      var.each do |v|
        name_clean = get_prepared_name(v['expender'])
        name_type = det.determine(name_clean)
        name_clean = get_clean_name(name_clean, name_type == 'Person' ? :ind : :org)
        insert << "('#{escape(v['expender'])}', '#{escape(name_clean)}', '#{name_type}', '#{v['created_at']}'),"
      end
      puts "#{insert.chop};"
      route.query("#{insert.chop};")
    end
  end
end
