# Creator:      Daniel Moskalchuk
# Migrated by:  Sergii Butrymenko
# Dataset Name: Delinquent Taxes
# Task #:       37
# Created:      --
# Migrated:     July 2021

# ruby mlc.rb --tool="clean::il::delinquent_taxes"

def message_to_slack(message)
  Slack::Web::Client.new.chat_postMessage(
    channel: 'UKLB1JGDN',
    text: "*[CLEANING #37] Delinquent Taxes* \n>#{message}",
    as_user: true
  )
end

def escape(str)
  if str.to_s.empty?
    str
  else
    str.to_s.gsub(/\\/, '\&\&').gsub(/'/, "''")
  end
end

def execute(options = {})
  tab = 'assessor_delinquent_taxes'
  db = C::Mysql.on(DB08, 'cook_il_raw')

  all_cases = db.query("SELECT * FROM #{tab} WHERE taxpayer_name_clean IS NULL AND Taxpayer_Name NOT REGEXP 'TAX\s*PAYER(\s+OF)?|CURRENT OWNER';")
  cases_count = all_cases.count
  if cases_count.zero?
    message_to_slack('There are no new cases to clean')
  else
    det = MiniLokiC::Formatize::Determiner.new
    all_cases.each_with_index do |each_row, i|
      # puts "#{i+1}/#{cases_count} - ID: #{each_row['id']}".black.on_cyan
      next if each_row['Taxpayer_Name'].length == 0

      type = det.determine(each_row['Taxpayer_Name'])
      name = if type == 'Person'
               MiniLokiC::Formatize::Cleaner.person_clean(each_row['Taxpayer_Name'], false).gsub(/\d+/, "").rstrip.lstrip
             else
               MiniLokiC::Formatize::Cleaner.org_clean(each_row['Taxpayer_Name'])
                    # .sub(/^A&a /, 'A&A ').sub(/& a /, '& A ').gsub('. , ', '., ').gsub(/L.?L.?C.?/, "LLC")
             end
                  # .gsub(/"/, '')
      next unless name

      query = <<~SQL
        UPDATE #{tab}
        SET taxpayer_name_clean = '#{escape(name)}', taxpayer_class = '#{type}'
        WHERE id = #{each_row['id']};
      SQL
      # puts each_row['Taxpayer_Name'].green
      # puts query.red
      # puts "#{each_row['taxpayer_name_clean']} -----#{each_row['taxpayer_class']}"
      # puts "#{each_row['Taxpayer_Name'].green} ---- #{each_row['taxpayer_name_clean']} ---- #{each_row['taxpayer_class']} ---- #{name} ---- #{type}"
      db.query(query)
    end
    message_to_slack("#{cases_count} names were cleaned")
  end
  db.close
end
