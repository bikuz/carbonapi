from django.contrib import admin
from .models import Physiography, ForestSpecies, HDModel, SpeciesHDModelMap, Project, Plot, Allometric


class PhysiographyAdmin(admin.ModelAdmin):
    ordering = ('code',)
    list_display=('name','ecological','code')    
    search_fields = ('code','name','ecological')
     

class ForestSpeciesAdmin(admin.ModelAdmin):
    ordering = ('species_name','code')
    list_display=('species_name','code','species','family','form','nepal','altitude','local_name')    
    search_fields = ('code','species_name','species','family','local_name' )
    list_filter = ('species','family') 

class HDModelAdmin(admin.ModelAdmin):
    ordering = ('code',)
    list_display=('name','code','expression','description')    
    search_fields = ('code','name')

class SpeciesHDModelMapAdmin(admin.ModelAdmin):
    ordering = ('physiography', 'species',)
    list_display = (
        'get_species_code', 
        'get_species_name',
        'hd_model',
        'physiography',
        'get_hd_a',
        'get_hd_b',
        'get_hd_c'
    )
    search_fields = (
        'species__code',
        'species__species_name',
        'hd_model__name',
        'physiography__name'
    )
    list_filter = (
        'hd_model',
        'physiography'
    )
    class Media:
        css = {
            'all': ('admin/css/admin_custom.css',)
        }
    
    # Custom methods for species info
    def get_species_code(self, obj):
        return obj.species.code
    get_species_code.short_description = 'Species Code'
    get_species_code.admin_order_field = 'species__code'
    
    def get_species_name(self, obj):
        return obj.species.species_name
    get_species_name.short_description = 'Species Name'
    get_species_name.admin_order_field = 'species__species_name'
    
    # Custom methods for simplified hd parameter display
    def get_hd_a(self, obj):
        return obj.hd_a
    get_hd_a.short_description = '\u200ba' 
    get_hd_a.admin_order_field = 'hd_a'
    
    def get_hd_b(self, obj):
        return obj.hd_b
    get_hd_b.short_description = '\u200bb'
    get_hd_b.admin_order_field = 'hd_b'
    
    def get_hd_c(self, obj):
        return obj.hd_c
    get_hd_c.short_description = '\u200bc'
    get_hd_c.admin_order_field = 'hd_c'

class ProjectAdmin(admin.ModelAdmin):
    list_display = ('name', 'status', 'current_phase', 'current_step', 'created_by', 'created_date', 'last_modified')
    list_filter = ('status', 'current_phase', 'created_date')
    search_fields = ('name', 'description')
    readonly_fields = ('created_date', 'last_modified')
    fieldsets = (
        ('Basic Information', {
            'fields': ('name', 'description')
        }),
        ('Status & Progress', {
            'fields': ('status', 'current_phase', 'current_step')
        }),
        ('Metadata', {
            'fields': ('created_by', 'created_date', 'last_modified'),
            'classes': ('collapse',)
        }),
    )
    
    def get_queryset(self, request):
        return super().get_queryset(request).select_related('created_by')
    
    def save_model(self, request, obj, form, change):
        if not change:  # Only set created_by on creation
            obj.created_by = request.user
        super().save_model(request, obj, form, change)

class PlotAdmin(admin.ModelAdmin):
    list_display = ('plot_id', 'col', 'row', 'plot_number', 'lat', 'lon', 'phy_zone', 'province', 'province_id')
    list_filter = ('phy_zone', 'province_id')
    search_fields = ('plot_id', 'col', 'row', 'plot_number', 'province')
    ordering = ('plot_id',)
    readonly_fields = ('plot_id',)

class AllometricAdmin(admin.ModelAdmin):
    list_display = (
        'get_species_code',
        'get_species_name',
        'density',
        'stem_a',
        'stem_b',
        'stem_c'
    )
    list_filter = ('species',)
    search_fields = (
        'species__code',
        'species__species_name',
        'species__scientific_name'
    )
    ordering = ('species__code',)
    
    fieldsets = (
        ('Species Information', {
            'fields': ('species', 'density')
        }),
        ('Stem Allometric Parameters', {
            'fields': ('stem_a', 'stem_b', 'stem_c'),
            'classes': ('collapse',)
        }),
        ('Top 10% Allometric Parameters', {
            'fields': ('top_10_a', 'top_10_b'),
            'classes': ('collapse',)
        }),
        ('Top 20% Allometric Parameters', {
            'fields': ('top_20_a', 'top_20_b'),
            'classes': ('collapse',)
        }),
        ('Bark Stem Allometric Parameters', {
            'fields': ('bark_stem_a', 'bark_stem_b'),
            'classes': ('collapse',)
        }),
        ('Bark Top 10% Allometric Parameters', {
            'fields': ('bark_top_10_a', 'bark_top_10_b'),
            'classes': ('collapse',)
        }),
        ('Bark Top 20% Allometric Parameters', {
            'fields': ('bark_top_20_a', 'bark_top_20_b'),
            'classes': ('collapse',)
        }),
        ('Branch Allometric Parameters', {
            'fields': ('branch_s', 'branch_m', 'branch_l'),
            'classes': ('collapse',)
        }),
        ('Foliage Allometric Parameters', {
            'fields': ('foliage_s', 'foliage_m', 'foliage_l'),
            'classes': ('collapse',)
        }),
    )
    
    def get_species_code(self, obj):
        return obj.species.code
    get_species_code.short_description = 'Species Code'
    get_species_code.admin_order_field = 'species__code'
    
    def get_species_name(self, obj):
        return obj.species.species_name
    get_species_name.short_description = 'Species Name'
    get_species_name.admin_order_field = 'species__species_name'

# Register your models here.
admin.site.register(Physiography,PhysiographyAdmin)
admin.site.register(ForestSpecies,ForestSpeciesAdmin)
admin.site.register(HDModel,HDModelAdmin)
admin.site.register(SpeciesHDModelMap,SpeciesHDModelMapAdmin)
admin.site.register(Project, ProjectAdmin)
admin.site.register(Plot, PlotAdmin)
admin.site.register(Allometric, AllometricAdmin)